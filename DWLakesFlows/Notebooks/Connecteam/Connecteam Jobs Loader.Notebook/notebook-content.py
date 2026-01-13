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

# ============================================================================
# CONNECTEAM JOBS DIMENSION ETL
# ============================================================================
# Purpose: Extract job dimension data from Connecteam Jobs API
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Created: 2025-11-12
# Last Updated: 2025-12-03
# ============================================================================

import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, explode_outer, coalesce, array_size, when, trim, 
    concat_ws, size, array_join, regexp_replace, from_json
)
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, IntegerType, TimestampType
import time
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access

# ============================================================================
# CONFIGURATION
# ============================================================================

# Azure Key Vault Configuration
KEY_VAULT_URL = "https://itt-dataanalytics.vault.azure.net/"
SECRET_NAME = "ConnecteamKeyMBrents"

# Retrieve API token from Key Vault
print("Retrieving Connecteam API token from Azure Key Vault...")
try:
    API_KEY = mssparkutils.credentials.getSecret(KEY_VAULT_URL, SECRET_NAME)
    print("✓ Successfully retrieved API token from Key Vault\n")
except Exception as e:
    print(f"✗ Error retrieving API token from Key Vault: {str(e)}")
    print("  Make sure:")
    print("  1. The Key Vault is linked to your Fabric workspace")
    print("  2. You have 'Get' permissions on the secret")
    print("  3. The secret name is correct: ConnecteamKeyMBrents")
    raise

# API Configuration
BASE_URL = "https://api.connecteam.com/jobs/v1/jobs"
HEADERS = {
    "accept": "application/json",
    "X-API-Key": API_KEY
}

# API Request Configuration
REQUEST_TIMEOUT = 30  # Seconds for API request timeout
REQUEST_DELAY = 0.4  # Seconds between API calls

# Lakehouse Configuration
LAKEHOUSE_TABLE_NAME = "dim_connecteam_jobs"
RAW_JSON_PATH = "Files/raw/connecteam_jobs"

# Load Configuration
LOAD_TYPE = "overwrite"  # Options: "overwrite" or "merge"

# API Pagination
PAGE_SIZE = 500  # Max results per page

# Filter Configuration
INCLUDE_DELETED = False  # Set to True to include deleted jobs
SCHEDULER_IDS = []  # Leave empty for ALL jobs, or specify: [10684510, 10528727, ...]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def print_config():
    """Print current configuration"""
    print("=" * 80)
    print("CONNECTEAM JOBS DIMENSION ETL")
    print("=" * 80)
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Load strategy: {LOAD_TYPE.upper()}")
    print(f"Target table: {LAKEHOUSE_TABLE_NAME}")
    print(f"Include deleted jobs: {INCLUDE_DELETED}")
    if SCHEDULER_IDS:
        print(f"Scheduler filter: {len(SCHEDULER_IDS)} specific schedulers")
    else:
        print("Scheduler filter: ALL schedulers")
    print("=" * 80 + "\n")


def table_exists(table_name):
    """Check if table exists in Lakehouse"""
    try:
        spark.table(table_name)
        return True
    except:
        return False


def fetch_all_jobs():
    """Fetch all jobs from Connecteam API with pagination"""
    print("\n" + "=" * 80)
    print("EXTRACTING JOBS FROM CONNECTEAM API")
    print("=" * 80)
    
    all_jobs = []
    offset = 0
    page = 1
    
    # Build query parameters
    params = {
        "includeDeleted": str(INCLUDE_DELETED).lower(),
        "order": "asc",
        "limit": PAGE_SIZE,
        "offset": offset
    }
    
    # Add scheduler filter if specified
    if SCHEDULER_IDS:
        params["instanceIds"] = ",".join(map(str, SCHEDULER_IDS))
    
    while True:
        print(f"  Fetching page {page} (offset: {offset})...")
        params["offset"] = offset
        
        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            jobs = data.get("data", {}).get("jobs", [])
            jobs_count = len(jobs)
            
            all_jobs.extend(jobs)
            print(f"    Retrieved {jobs_count} jobs (Total: {len(all_jobs)})")
            
            # Check if we've reached the last page
            if jobs_count < PAGE_SIZE:
                print(f"  Last page reached (got {jobs_count} < {PAGE_SIZE})")
                break
            
            offset += PAGE_SIZE
            page += 1
            time.sleep(REQUEST_DELAY)
            
        except requests.exceptions.RequestException as e:
            print(f"✗ ERROR fetching jobs: {e}")
            raise
    
    print(f"✓ Completed: {len(all_jobs)} total jobs retrieved\n")
    return all_jobs


def save_raw_json(jobs_data):
    """Save raw JSON to Lakehouse Files"""
    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"{timestamp}_connecteam_jobs_raw.json"
    filepath = f"{RAW_JSON_PATH}/{filename}"
    
    print(f"Saving raw JSON to: {filepath}")
    
    try:
        # Convert to JSON string
        json_str = json.dumps({"jobs": jobs_data, "extracted_at": datetime.now().isoformat()}, indent=2)
        
        # Write to Lakehouse Files using notebookutils
        mssparkutils.fs.put(filepath, json_str, True)
        
        print(f"  ✓ Raw JSON written to Lakehouse: {filepath}\n")
    except Exception as e:
        print(f"  ⚠ Warning: Could not save raw JSON: {e}\n")


def transform_jobs(jobs_data):
    """Transform jobs JSON data into DataFrame"""
    print("Starting transformation...")
    
    if not jobs_data:
        print("  ⚠ No jobs data to transform")
        return None
    
    # Create DataFrame from JSON using spark.read.json
    rdd = spark.sparkContext.parallelize([json.dumps(job) for job in jobs_data])
    df = spark.read.json(rdd)
    
    print(f"  Initial records: {df.count()} | Columns: {len(df.columns)}")
    
    # Transform the data
    df_transformed = df.select(
        col("jobId").alias("job_id"),
        col("title").alias("job_title"),
        col("code").alias("job_code"),
        col("color").alias("job_color"),
        # Strip HTML tags from description
        regexp_replace(col("description"), "<[^>]+>", "").alias("job_description"),
        col("gps.address").alias("job_address"),
        col("isDeleted").alias("is_deleted"),
        col("assign.type").alias("assign_type"),
        # Convert arrays to comma-separated strings for easier querying
        when(size(col("assign.userIds")) > 0, 
             array_join(col("assign.userIds").cast("array<string>"), ",")
        ).otherwise(lit(None)).alias("assigned_user_ids"),
        when(size(col("assign.groupIds")) > 0,
             array_join(col("assign.groupIds").cast("array<string>"), ",")
        ).otherwise(lit(None)).alias("assigned_group_ids"),
        coalesce(size(col("assign.userIds")), lit(0)).alias("assigned_user_count"),
        coalesce(size(col("assign.groupIds")), lit(0)).alias("assigned_group_count"),
        col("useParentData").alias("use_parent_data"),
        # Keep instance IDs as comma-separated string
        when(size(col("instanceIds")) > 0,
             array_join(col("instanceIds").cast("array<string>"), ",")
        ).otherwise(lit(None)).alias("instance_ids"),
        coalesce(size(col("instanceIds")), lit(0)).alias("instance_count"),
        # Add ETL metadata
        lit(datetime.now()).cast("timestamp").alias("etl_loaded_datetime")
    )
    
    print(f"  Final records: {df_transformed.count()}")
    print(f"  Final columns: {len(df_transformed.columns)}\n")
    
    return df_transformed


def load_to_lakehouse(df, table_name, load_type="overwrite"):
    """Load DataFrame to Lakehouse table"""
    print("Loading data to Lakehouse...")
    print(f"  Table name: {table_name} | Load type: {load_type}")
    print(f"  Incoming records: {df.count()}")
    
    # Enable schema evolution for Delta Lake (Fabric best practice)
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    if load_type == "overwrite":
        print(f"  Performing OVERWRITE operation...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        record_count = spark.table(table_name).count()
        print(f"  Table now has {record_count:,} records")
        print(f"  ✓ Overwrite completed successfully\n")
        
    elif load_type == "merge":
        if not table_exists(table_name):
            print(f"  ⚠ Table does not exist, creating with initial load...")
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            record_count = spark.table(table_name).count()
            print(f"  Table created with {record_count:,} records")
            print(f"  ✓ Initial load completed successfully\n")
        else:
            print(f"  Performing MERGE operation...")
            from delta.tables import DeltaTable
            
            target = DeltaTable.forName(spark, table_name)
            
            # Merge on job_id
            target.alias("t").merge(
                df.alias("s"),
                "t.job_id = s.job_id"
            ).whenMatchedUpdateAll(
            ).whenNotMatchedInsertAll(
            ).execute()
            
            record_count = spark.table(table_name).count()
            print(f"  Table now has {record_count:,} records")
            print(f"  ✓ Merge completed successfully\n")
    else:
        raise ValueError(f"Invalid load_type: {load_type}")
    
    # Show sample
    print("Sample of loaded data:")
    spark.table(table_name).show(5, truncate=False)
    print()
    
    # Show schema
    print("Schema:")
    spark.table(table_name).printSchema()
    print()


def main():
    """Main ETL process"""
    print("=" * 80)
    print("CONNECTEAM JOBS DIMENSION ETL")
    print("=" * 80)
    print(f"Execution started: {datetime.now()}\n")
    print_config()
    
    try:
        # Step 1: Extract
        jobs_data = fetch_all_jobs()
        
        if not jobs_data:
            print("⚠ No jobs data retrieved. Exiting.")
            return
        
        # Step 2: Save raw JSON
        save_raw_json(jobs_data)
        
        # Step 3: Transform
        df_jobs = transform_jobs(jobs_data)
        
        if df_jobs is None:
            print("✗ Transformation failed. Exiting.")
            return
        
        # Step 4: Load
        load_to_lakehouse(df_jobs, LAKEHOUSE_TABLE_NAME, load_type=LOAD_TYPE)
        
        # Step 5: Summary
        print("=" * 80)
        print("ETL COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"Completion time: {datetime.now()}")
        print(f"Total jobs loaded: {df_jobs.count():,}")
        
        # Show breakdown by deleted status
        print("\nBreakdown by status:")
        df_summary = spark.table(LAKEHOUSE_TABLE_NAME).groupBy("is_deleted").count().orderBy("is_deleted")
        df_summary.show()
        
        print("=" * 80 + "\n")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("✗ ETL FAILED")
        print("=" * 80)
        print(f"Error: {str(e)}")
        print("=" * 80 + "\n")
        raise


# ============================================================================
# EXECUTION
# ============================================================================

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    main()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
