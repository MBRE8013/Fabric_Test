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

# Fabric Notebook: Workable Stages Dimension Loader
# Purpose: Load hiring stage data from Workable API and create dimension table for Power BI
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Last Updated: 2025-12-03

# %%
# Setup and Configuration

import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access

# Initialize Spark session
spark = SparkSession.builder.appName("WorkableStagesLoader").getOrCreate()

# %%
# Configuration Parameters

# Azure Key Vault Configuration
KEY_VAULT_URL = "https://itt-dataanalytics.vault.azure.net/"
SECRET_NAME = "WorkableKeyMBrents"

# Retrieve API token from Key Vault
print("Retrieving Workable API token from Azure Key Vault...")
try:
    WORKABLE_API_TOKEN = mssparkutils.credentials.getSecret(KEY_VAULT_URL, SECRET_NAME)
    print("✓ Successfully retrieved API token from Key Vault")
except Exception as e:
    print(f"✗ Error retrieving API token from Key Vault: {str(e)}")
    print("  Make sure:")
    print("  1. The Key Vault is linked to your Fabric workspace")
    print("  2. You have 'Get' permissions on the secret")
    print("  3. The secret name is correct: WorkableKeyMBrents")
    raise

# Workable API Configuration
WORKABLE_SUBDOMAIN = "intellatriage"  # Your Workable subdomain
WORKABLE_API_BASE = f"https://{WORKABLE_SUBDOMAIN}.workable.com/spi/v3"

# Target table name (will be created in default lakehouse)
TARGET_TABLE = "dim_workable_stages"

# API endpoint
STAGES_ENDPOINT = f"{WORKABLE_API_BASE}/stages"

# %%
# Define API Request Function

def get_workable_stages(api_token):
    """
    Fetch stages from Workable API with retry logic
    
    Args:
        api_token (str): Workable API bearer token
    
    Returns:
        list: List of stage dictionaries
    """
    headers = {
        'Authorization': f'Bearer {api_token}',
        'accept': 'application/json'
    }
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,  # Maximum 3 retry attempts
        backoff_factor=1,  # Wait 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP status codes
        allowed_methods=["GET"]  # Only retry GET requests
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    
    try:
        print(f"Fetching stages from: {STAGES_ENDPOINT}")
        response = session.get(STAGES_ENDPOINT, headers=headers, timeout=30)
        
        # Check response status
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        print(f"✓ Successfully fetched {len(data.get('stages', []))} stages")
        return data.get('stages', [])
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching stages after retries: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text}")
        raise
    finally:
        session.close()

# %%
# Fetch Stages Data

# Fetch stages from API
stages_data = get_workable_stages(WORKABLE_API_TOKEN)

# Display sample data
print("\n" + "="*80)
print("SAMPLE STAGE DATA:")
print("="*80)
print(json.dumps(stages_data[:2] if len(stages_data) > 0 else [], indent=2))

# %%
# Transform Data for Dimension Table

def transform_stages_data(stages_list):
    """
    Transform raw stages data into dimension table structure
    
    Args:
        stages_list (list): Raw stages data from API
    
    Returns:
        pd.DataFrame: Cleaned and structured DataFrame
    """
    if not stages_list:
        print("⚠ Warning: No stages data to transform")
        return pd.DataFrame()
    
    # Create DataFrame from stages list
    df = pd.DataFrame(stages_list)
    
    # Add metadata columns
    df['etl_loaded_at'] = datetime.now()
    df['etl_source'] = 'workable_api'
    
    # Ensure consistent column names (snake_case)
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    print(f"\n✓ Transformed {len(df)} stages")
    print(f"Columns: {', '.join(df.columns.tolist())}")
    
    return df

# %%
# Transform and display the data

# Transform the data
df_pandas = transform_stages_data(stages_data)

# Display the transformed data
print("\n" + "="*80)
print("TRANSFORMED STAGES DATA:")
print("="*80)
print(df_pandas.head(10))
print(f"\nShape: {df_pandas.shape}")

# %%
# Create Spark DataFrame with Schema

# Define schema based on expected Workable API response
# Adjust this schema based on actual API response structure
stage_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("kind", StringType(), True),  # e.g., "sourced", "applied", "phone_interview"
    StructField("etl_loaded_at", TimestampType(), True),
    StructField("etl_source", StringType(), True)
])

# Convert to Spark DataFrame
if not df_pandas.empty:
    # Convert pandas DataFrame to Spark DataFrame
    df_spark = spark.createDataFrame(df_pandas)
    
    # Show schema and data
    print("\n" + "="*80)
    print("SPARK DATAFRAME SCHEMA:")
    print("="*80)
    df_spark.printSchema()
    
    print("\n" + "="*80)
    print("SPARK DATAFRAME PREVIEW:")
    print("="*80)
    df_spark.show(truncate=False)
    
    print(f"\n✓ Created Spark DataFrame with {df_spark.count()} rows")
else:
    print("⚠ Warning: Empty DataFrame - no data to create Spark DataFrame")

# %%
# Validate Data Before Write

# Critical check: Ensure we have data before overwriting table
if df_pandas.empty or len(stages_data) == 0:
    error_msg = """
    ⚠ CRITICAL: API returned no stages data!
    
    Aborting table write to prevent data loss.
    
    Possible causes:
    1. API authentication failed but returned 200 status
    2. Workable API is experiencing issues
    3. All stages were deleted (unlikely)
    
    Action required: Investigate API response before re-running.
    """
    print(error_msg)
    raise ValueError("Empty API response - aborting to prevent data loss")

# Additional validation: Check for minimum expected stages
MIN_EXPECTED_STAGES = 5  # Adjust based on your typical stage count
if len(stages_data) < MIN_EXPECTED_STAGES:
    warning_msg = f"""
    ⚠ WARNING: Only {len(stages_data)} stages found (expected at least {MIN_EXPECTED_STAGES})
    
    This may indicate:
    - Incomplete API response
    - Stages were recently deleted
    - API pagination issue
    
    Review the data before proceeding. Continuing in 10 seconds...
    Press Ctrl+C to abort if this looks incorrect.
    """
    print(warning_msg)
    time.sleep(10)  # Give user time to abort if running interactively

print(f"✓ Validation passed: {len(stages_data)} stages ready for load")

# %%
# Write to Delta Table (Managed Table)

if not df_pandas.empty:
    try:
        # Write as managed Delta table (this will appear in dbo schema in Power BI)
        df_spark.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(TARGET_TABLE)
        
        print(f"\n✓ Successfully created table: {TARGET_TABLE}")
        print(f"  - Mode: overwrite")
        print(f"  - Format: Delta")
        print(f"  - Rows: {df_spark.count()}")
        
    except Exception as e:
        print(f"\n✗ Error writing to Delta table: {str(e)}")
        raise
else:
    print("⚠ Skipping table write - no data available")

# %%
# Verify Table Creation

# Query the table to verify
try:
    verify_df = spark.sql(f"SELECT * FROM {TARGET_TABLE}")
    
    print("\n" + "="*80)
    print(f"VERIFICATION: {TARGET_TABLE}")
    print("="*80)
    print(f"Total Rows: {verify_df.count()}")
    print("\nSample Data:")
    verify_df.show(10, truncate=False)
    
    print("\n" + "="*80)
    print("SUMMARY STATISTICS:")
    print("="*80)
    
    # Show stage counts by type if 'kind' column exists
    if 'kind' in verify_df.columns:
        print("\nStages by Type:")
        verify_df.groupBy("kind").count().orderBy("count", ascending=False).show()
    
    # Show stages by position
    if 'position' in verify_df.columns:
        print("\nStages by Position (Order):")
        verify_df.select("position", "name", "kind").orderBy("position").show(truncate=False)
    
except Exception as e:
    print(f"✗ Error verifying table: {str(e)}")

# %%
# Data Quality Checks

def run_data_quality_checks(table_name):
    """
    Run data quality checks on the stages dimension table
    """
    print("\n" + "="*80)
    print("DATA QUALITY CHECKS")
    print("="*80)
    
    df = spark.sql(f"SELECT * FROM {table_name}")
    
    # Check 1: Row count
    row_count = df.count()
    print(f"\n✓ Total Rows: {row_count}")
    
    # Check 2: Null values
    print("\n✓ Null Value Check:")
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"  - {column}: {null_count} nulls ({null_count/row_count*100:.1f}%)")
        else:
            print(f"  - {column}: No nulls ✓")
    
    # Check 3: Duplicate slugs (stages use slug as identifier, not id)
    duplicate_count = df.groupBy("slug").count().filter(col("count") > 1).count()
    if duplicate_count > 0:
        print(f"\n⚠ Warning: {duplicate_count} duplicate stage slugs found")
    else:
        print(f"\n✓ No duplicate stage slugs")
    
    # Check 4: Position sequence
    if 'position' in df.columns:
        positions = df.select("position").distinct().orderBy("position").collect()
        position_values = [row.position for row in positions if row.position is not None]
        
        if position_values:
            expected_positions = list(range(min(position_values), max(position_values) + 1))
            missing_positions = set(expected_positions) - set(position_values)
            
            if missing_positions:
                print(f"\n⚠ Warning: Missing positions in sequence: {missing_positions}")
            else:
                print(f"\n✓ Position sequence is continuous: {min(position_values)} to {max(position_values)}")
    
    print("\n" + "="*80)
    print("DATA QUALITY CHECKS COMPLETE")
    print("="*80)

# Run the checks
try:
    run_data_quality_checks(TARGET_TABLE)
except Exception as e:
    print(f"Error running data quality checks: {str(e)}")

# %%
# Completion Summary

print("\n" + "="*80)
print("WORKABLE STAGES LOADER - COMPLETION SUMMARY")
print("="*80)
print(f"✓ API Endpoint: {STAGES_ENDPOINT}")
print(f"✓ Target Table: {TARGET_TABLE}")
print(f"✓ Rows Loaded: {df_spark.count() if not df_pandas.empty else 0}")
print(f"✓ Load Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\nNext Steps:")
print("1. Verify table appears in Power BI under 'dbo' schema")
print("2. Create relationships with fact tables (e.g., candidates, applications)")
print("3. Build visualizations using stage progression metrics")
print("4. Schedule this notebook to run weekly (or when stages change)")
print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
