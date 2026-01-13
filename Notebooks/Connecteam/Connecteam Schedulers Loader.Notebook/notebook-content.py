# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f594b6ab-709b-4089-9f71-44f9bdb59e68",
# META       "default_lakehouse_name": "IntellaTriageLakehouse",
# META       "default_lakehouse_workspace_id": "6f688f9a-787a-46c6-8068-8d6e88edd051",
# META       "known_lakehouses": [
# META         {
# META           "id": "f594b6ab-709b-4089-9f71-44f9bdb59e68"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook: Connecteam Schedulers Dimension Loader
# Purpose: Load scheduler metadata from Connecteam API into a static dimension table
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Last Updated: 2025-11-13

import requests
import pandas as pd
import json
import os
from datetime import datetime
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# =============================================================================
# CONFIGURATION
# =============================================================================

# Azure Key Vault Configuration
KEY_VAULT_URL = "https://itt-dataanalytics.vault.azure.net/"
SECRET_NAME = "ConnecteamKeyMBrents"

# Retrieve API token from Key Vault
print("Retrieving Connecteam API token from Azure Key Vault...")
try:
    API_TOKEN = mssparkutils.credentials.getSecret(KEY_VAULT_URL, SECRET_NAME)
    print("✓ Successfully retrieved API token from Key Vault\n")
except Exception as e:
    print(f"✗ Error retrieving API token from Key Vault: {str(e)}")
    print("  Make sure:")
    print("  1. The Key Vault is linked to your Fabric workspace")
    print("  2. You have 'Get' permissions on the secret")
    print("  3. The secret name is correct: ConnecteamKeyMBrents")
    raise

SCHEDULERS_URL = "https://api.connecteam.com/scheduler/v1/schedulers"

# Lakehouse configuration
DATABASE = os.getenv("FABRIC_DB", "")
LAKEHOUSE_TABLE_NAME = "dim_schedulers" if not DATABASE else f"{DATABASE}.dim_schedulers"
LAKEHOUSE_RAW_DIR = "Files/raw/connecteam_schedulers"

# Which schedulers to include in shift loads (by name or ID)
# Use None to include all non-archived, or specify a list
INCLUDE_SCHEDULER_NAMES = [
    "Nurse Advice Line",
    "Supervisor Schedule", 
    "Triage Schedule",
    "CSS Schedule",
    "Training Schedule",
    "AOC Schedule"
]

# Exclude specific schedulers even if they match the include list
EXCLUDE_SCHEDULER_NAMES = [
    "Sandbox Scheduler",
    "CSS Test",
    "Schedule"  # Generic name, probably a test
]

# =============================================================================
# EXTRACT
# =============================================================================

def get_schedulers(api_token):
    """Fetch all schedulers from Connecteam API"""
    headers = {"X-API-Key": api_token, "Content-Type": "application/json"}
    
    print("="*80)
    print("FETCHING SCHEDULERS FROM CONNECTEAM API")
    print("="*80)
    
    try:
        resp = requests.get(SCHEDULERS_URL, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        schedulers = data.get("data", {}).get("schedulers", [])
        print(f"✓ Retrieved {len(schedulers)} total schedulers")
        
        return schedulers, data
        
    except requests.exceptions.RequestException as e:
        print(f"ERROR: API request failed: {e}")
        raise

# =============================================================================
# TRANSFORM
# =============================================================================

def transform_schedulers(schedulers_data):
    """Transform schedulers JSON into DataFrame with filtering logic"""
    print("\n" + "="*80)
    print("TRANSFORMING SCHEDULER DATA")
    print("="*80)
    
    if not schedulers_data:
        print("WARNING: No schedulers data to transform")
        return None
    
    # Convert to DataFrame
    pdf = pd.DataFrame(schedulers_data)
    
    # Add load metadata
    pdf['etl_loaded_datetime'] = datetime.now()
    pdf['is_archived'] = pdf['isArchived']
    pdf['scheduler_name'] = pdf['name']
    
    # Apply filtering logic
    pdf['include_in_shift_loads'] = False
    
    # Start with non-archived schedulers
    mask = ~pdf['is_archived']
    
    # Apply include list if specified
    if INCLUDE_SCHEDULER_NAMES:
        mask = mask & pdf['scheduler_name'].isin(INCLUDE_SCHEDULER_NAMES)
    
    # Apply exclude list
    if EXCLUDE_SCHEDULER_NAMES:
        mask = mask & ~pdf['scheduler_name'].isin(EXCLUDE_SCHEDULER_NAMES)
    
    pdf.loc[mask, 'include_in_shift_loads'] = True
    
    # Select and rename columns for clean dimension table
    pdf = pdf[[
        'schedulerId',
        'scheduler_name', 
        'timezone',
        'is_archived',
        'include_in_shift_loads',
        'etl_loaded_datetime'
    ]].rename(columns={'schedulerId': 'scheduler_id'})
    
    print(f"\nScheduler Summary:")
    print(f"  Total schedulers: {len(pdf)}")
    print(f"  Archived: {pdf['is_archived'].sum()}")
    print(f"  Active: {(~pdf['is_archived']).sum()}")
    print(f"  Included in shift loads: {pdf['include_in_shift_loads'].sum()}")
    
    print(f"\n✓ Schedulers INCLUDED in shift loads:")
    included = pdf[pdf['include_in_shift_loads']][['scheduler_id', 'scheduler_name', 'timezone']]
    for _, row in included.iterrows():
        print(f"    • {row['scheduler_id']:>8} - {row['scheduler_name']} ({row['timezone']})")
    
    print(f"\n⊘ Schedulers EXCLUDED from shift loads:")
    excluded = pdf[~pdf['include_in_shift_loads']][['scheduler_id', 'scheduler_name', 'is_archived']]
    for _, row in excluded.iterrows():
        archived_flag = " [ARCHIVED]" if row['is_archived'] else ""
        print(f"    • {row['scheduler_id']:>8} - {row['scheduler_name']}{archived_flag}")
    
    return spark.createDataFrame(pdf)

# =============================================================================
# LOAD
# =============================================================================

def write_raw_json_to_lakehouse(path, payload_str):
    """Write raw JSON to Fabric Lakehouse Files"""
    try:
        try:
            mssparkutils.fs.mkdirs(os.path.dirname(path))
        except Exception:
            pass
        mssparkutils.fs.put(path, payload_str, overwrite=True)
        print(f"✓ Raw JSON written to: {path}")
    except Exception as e:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path.replace('Files/', ''), 'w', encoding='utf-8') as f:
            f.write(payload_str)
        print(f"⚠ Wrote to local disk: {path}")

def load_to_lakehouse(df, table_name):
    """Overwrite the dimension table (this is a small, static table)"""
    print("\n" + "="*80)
    print("LOADING TO LAKEHOUSE")
    print("="*80)
    print(f"Target table: {table_name}")
    print(f"Load strategy: OVERWRITE (dimension table)")
    
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    
    final_count = spark.table(table_name).count()
    print(f"✓ Table loaded with {final_count} records")
    
    print("\nFinal table contents:")
    spark.table(table_name).orderBy("scheduler_name").show(20, truncate=False)

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "="*80)
    print("CONNECTEAM SCHEDULERS DIMENSION LOADER")
    print("="*80)
    print(f"Execution started: {datetime.now()}")
    
    # Step 1: Extract
    schedulers_data, raw_response = get_schedulers(API_TOKEN)
    
    # Step 1b: Save raw JSON
    raw_file_path = f"{LAKEHOUSE_RAW_DIR}/{datetime.now():%Y-%m-%d}_schedulers_raw.json"
    print(f"\nSaving raw JSON to: {raw_file_path}")
    write_raw_json_to_lakehouse(raw_file_path, json.dumps(raw_response, indent=2))
    
    # Step 2: Transform
    df_schedulers = transform_schedulers(schedulers_data)
    
    if df_schedulers is None:
        print("ERROR: No data to load. Exiting.")
        return
    
    # Step 3: Load
    load_to_lakehouse(df_schedulers, LAKEHOUSE_TABLE_NAME)
    
    print("\n" + "="*80)
    print("DIMENSION LOAD COMPLETED SUCCESSFULLY")
    print("="*80)
    print(f"Completion time: {datetime.now()}")
    print(f"\nNext step: Update your shifts loader to read from {LAKEHOUSE_TABLE_NAME}")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
