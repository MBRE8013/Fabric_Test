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

# Fabric Notebook: Workable Candidates Loader with Incremental Updates
# Purpose: Load candidate data from Workable API and robustly upsert into Delta tables for Direct Lake
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Last Updated: 2025-11-18

import requests
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, when, lit, coalesce, current_timestamp, 
    concat_ws, sha2, to_timestamp
)
from pyspark.sql.types import *
from delta.tables import DeltaTable

# =============================================================================
# CONFIGURATION
# =============================================================================

# Azure Key Vault Configuration
KEY_VAULT_URL = "https://itt-dataanalytics.vault.azure.net/"
SECRET_NAME = "WorkableKeyMBrents"

# Retrieve API token from Key Vault
print("Retrieving Workable API token from Azure Key Vault...")
try:
    API_TOKEN = mssparkutils.credentials.getSecret(KEY_VAULT_URL, SECRET_NAME)
    print("✓ Successfully retrieved API token from Key Vault\n")
except Exception as e:
    print(f"✗ Error retrieving API token from Key Vault: {str(e)}")
    print("  Make sure:")
    print("  1. The Key Vault is linked to your Fabric workspace")
    print("  2. You have 'Get' permissions on the secret")
    print("  3. The secret name is correct: WorkableKeyMBrents")
    raise

# Workable API Configuration
WORKABLE_SUBDOMAIN = "intellatriage"
WORKABLE_API_BASE = f"https://{WORKABLE_SUBDOMAIN}.workable.com/spi/v3"
CANDIDATES_ENDPOINT = f"{WORKABLE_API_BASE}/candidates"

# =============================================================================
# LOAD STRATEGY CONFIGURATION
# =============================================================================

# Toggle for historical vs incremental load
HISTORICAL_BACKFILL = False  # Set to True for one-time backfill, False for daily incremental

if HISTORICAL_BACKFILL:
    # ONE-TIME HISTORICAL BACKFILL: 1/1/23 through today
    print("⚠ RUNNING IN HISTORICAL BACKFILL MODE")
    LOAD_TYPE = "overwrite"
    START_DATE = datetime(2023, 1, 1, 0, 0, 0)
    END_DATE = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
else:
    # DAILY INCREMENTAL LOAD: Last 3 days
    LOAD_TYPE = os.getenv("LOAD_TYPE", "merge")
    DAYS_BACK = int(os.getenv("DAYS_BACK", "3"))
    NOW = datetime.now()
    START_DATE = (NOW - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0, second=0, microsecond=0)
    END_DATE = NOW.replace(hour=23, minute=59, second=59, microsecond=0)

# Convert to Unix timestamps (seconds since epoch)
START_TIME_UNIX = int(START_DATE.timestamp())
END_TIME_UNIX = int(END_DATE.timestamp())

# Lakehouse configuration
DATABASE = os.getenv("FABRIC_DB", "")
LAKEHOUSE_TABLE_NAME = "workable_candidates" if not DATABASE else f"{DATABASE}.workable_candidates"

# Raw Files path (Fabric Lakehouse)
LAKEHOUSE_RAW_DIR = "Files/raw/workable_candidates"

# API params
LIMIT = 100  # Workable max is 100 per page
SORT_FIELD = "updated_at"  # "created_at" or "updated_at"
SORT_ORDER = "desc"  # "asc" or "desc"

# Rate limiting (Account tokens: 10 requests per 10 seconds)
# We'll be conservative and use 1.2 second delay between requests
RATE_LIMIT_DELAY = 1.2

# =============================================================================
# UTILS
# =============================================================================

def print_config():
    print("="*80)
    print("WORKABLE CANDIDATES INCREMENTAL LOAD CONFIGURATION")
    print("="*80)
    print(f"Current time: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Loading candidates updated from: {START_DATE:%Y-%m-%d %H:%M:%S}")
    print(f"                              to: {END_DATE:%Y-%m-%d %H:%M:%S}")
    print(f"Days covered: {(END_DATE - START_DATE).days + 1}")
    print(f"Load strategy: {LOAD_TYPE.upper()}")
    print(f"Target table: {LAKEHOUSE_TABLE_NAME}")
    print(f"Sort field/order: {SORT_FIELD}/{SORT_ORDER}")
    print(f"Rate limit delay: {RATE_LIMIT_DELAY}s between requests")
    print("="*80)

def ensure_automerge():
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def write_raw_json_to_lakehouse(path, payload_str):
    """Write raw JSON to Fabric Lakehouse Files via mssparkutils; fallback to local if unavailable."""
    try:
        from notebookutils import mssparkutils
        try:
            mssparkutils.fs.mkdirs(os.path.dirname(path))
        except Exception:
            pass
        mssparkutils.fs.put(path, payload_str, overwrite=True)
        print(f"  ✓ Raw JSON written to Lakehouse: {path}")
    except Exception as e:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path.replace('Files/', ''), 'w', encoding='utf-8') as f:
            f.write(payload_str)
        print(f"  ⚠ Wrote raw JSON to local disk (not Lakehouse): {path} | {e}")

def parse_workable_timestamp(ts_str):
    """Parse Workable ISO timestamp to datetime object"""
    if not ts_str:
        return None
    try:
        # Workable uses ISO format: "2016-04-01T03:01:43Z"
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        print(f"  ⚠ Failed to parse timestamp: {ts_str} | {e}")
        return None

# =============================================================================
# STEP 1: EXTRACT
# =============================================================================

def get_workable_candidates(api_token, updated_after=None, updated_before=None):
    """
    Fetch candidates from Workable API with pagination
    
    Args:
        api_token (str): Workable API bearer token
        updated_after (int): Unix timestamp - only candidates updated after this time
        updated_before (int): Unix timestamp - only candidates updated before this time
    
    Returns:
        list: List of candidate dictionaries
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "accept": "application/json"
    }
    
    all_candidates = []
    page_count = 0
    since_id = None
    
    print(f"\n{'='*80}")
    print(f"EXTRACTING CANDIDATES FROM WORKABLE")
    print(f"{'='*80}")
    if updated_after:
        print(f"Updated after: {datetime.fromtimestamp(updated_after)}")
    if updated_before:
        print(f"Updated before: {datetime.fromtimestamp(updated_before)}")
    
    while True:
        page_count += 1
        
        # Build query parameters
        params = {
            "limit": LIMIT
        }
        
        # Add time filters if provided
        if updated_after:
            params["updated_after"] = updated_after
        if updated_before:
            params["updated_before"] = updated_before
            
        # Add pagination using since_id
        if since_id:
            params["since_id"] = since_id
        
        try:
            print(f"  Fetching page {page_count}..." + (f" (since_id: {since_id})" if since_id else ""))
            
            response = requests.get(CANDIDATES_ENDPOINT, headers=headers, params=params, timeout=30)
            
            # Check for rate limiting
            if response.status_code == 429:
                reset_time = response.headers.get('X-Rate-Limit-Reset', 'unknown')
                print(f"  ⚠ Rate limit hit. Reset time: {reset_time}")
                print(f"  Waiting 15 seconds before retry...")
                time.sleep(15)
                continue
            
            response.raise_for_status()
            data = response.json()
            
            candidates = data.get("candidates", [])
            if not candidates:
                print("  No more candidates found. Stopping pagination.")
                break
            
            all_candidates.extend(candidates)
            print(f"    Retrieved {len(candidates)} candidates (Total: {len(all_candidates)})")
            
            # Check for next page using paging info
            paging = data.get("paging", {})
            next_url = paging.get("next")
            
            if not next_url:
                print(f"  Last page reached (no next URL)")
                break
            
            # Extract since_id from next URL
            # Format: "https://intellatriage.workable.com/spi/v3/candidates?limit=100&since_id=XXXXXX"
            if "since_id=" in next_url:
                since_id = next_url.split("since_id=")[1].split("&")[0]
            else:
                print(f"  ⚠ Cannot extract since_id from next URL: {next_url}")
                break
            
            # Respect rate limits
            time.sleep(RATE_LIMIT_DELAY)
            
        except requests.exceptions.RequestException as e:
            print(f"ERROR: API request failed on page {page_count}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response body: {e.response.text[:500]}")
            raise
    
    print(f"✓ Completed: {len(all_candidates)} total candidates extracted")
    return all_candidates

# =============================================================================
# STEP 2: TRANSFORM
# =============================================================================

def transform_candidates(candidates_data):
    """
    Transform raw candidates data into fact table structure
    
    Args:
        candidates_data (list): Raw candidates data from API
    
    Returns:
        pyspark.sql.DataFrame: Transformed Spark DataFrame
    """
    print("\nStarting transformation...")
    if not candidates_data:
        print("  No candidates data to transform")
        return None
    
    # Normalize nested JSON to pandas DataFrame
    pdf = pd.json_normalize(candidates_data)
    print(f"  Initial records: {len(pdf)} | Columns: {len(pdf.columns)}")
    
    # Convert to Spark DataFrame
    sdf = spark.createDataFrame(pdf)
    
    # Select and transform columns
    # Note: pandas json_normalize flattens nested fields with dots, so we use backticks
    transformed = sdf.select(
        # Composite key: candidate_id + job_shortcode
        col("id").alias("candidate_id"),
        col("`job.shortcode`").alias("job_shortcode"),
        
        # Candidate details
        col("name").alias("candidate_name"),
        col("firstname"),
        col("lastname"),
        col("headline"),
        col("email"),
        col("phone"),
        col("address"),
        
        # Account/company info
        col("`account.subdomain`").alias("account_subdomain"),
        col("`account.name`").alias("account_name"),
        
        # Job info
        col("`job.title`").alias("job_title"),
        
        # Stage and status
        col("stage"),
        col("stage_kind"),
        col("disqualified").cast("boolean"),
        col("disqualification_reason"),
        
        # Dates (convert from ISO string to timestamp)
        to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("created_at"),
        to_timestamp(col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("updated_at"),
        to_timestamp(col("hired_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("hired_at"),
        
        # Source tracking
        col("sourced").cast("boolean"),
        col("domain"),
        col("profile_url"),
        
        # Add ETL metadata
        current_timestamp().alias("etl_loaded_at"),
        lit(START_DATE).cast("timestamp").alias("window_start"),
        lit(END_DATE).cast("timestamp").alias("window_end")
    )
    
    # Add derived fields
    transformed = transformed.withColumn(
        "is_hired",
        when(col("hired_at").isNotNull(), lit(True)).otherwise(lit(False))
    ).withColumn(
        "days_since_created",
        ((col("etl_loaded_at").cast("long") - col("created_at").cast("long")) / 86400).cast("int")
    ).withColumn(
        "days_since_updated",
        ((col("etl_loaded_at").cast("long") - col("updated_at").cast("long")) / 86400).cast("int")
    )
    
    # Create composite key for merge operations
    transformed = transformed.withColumn(
        "candidate_job_key",
        sha2(
            concat_ws(
                "|",
                col("candidate_id"),
                coalesce(col("job_shortcode"), lit("__UNKNOWN__"))
            ),
            256
        )
    )
    
    count_rows = transformed.count()
    print(f"  Transformed to {count_rows} rows")
    print(f"  Final columns: {len(transformed.columns)}")
    
    return transformed

# =============================================================================
# STEP 3: LOAD
# =============================================================================

def table_exists(full_table_name: str) -> bool:
    """Check if a table exists in the Spark catalog"""
    if '.' in full_table_name:
        db, tbl = full_table_name.split('.', 1)
        return spark.catalog.tableExists(db, tbl)
    return spark.catalog.tableExists(full_table_name)

def load_to_lakehouse(df, table_name, load_type="overwrite", key_expr=None, delete_condition=None):
    """
    Load data to Lakehouse Delta table
    
    Args:
        df: Spark DataFrame to load
        table_name: Target table name
        load_type: "merge", "overwrite", or "append"
        key_expr: Key expression for merge (required if load_type="merge")
        delete_condition: Optional delete condition for merge
    """
    print("\nLoading data to Lakehouse...")
    print(f"  Table name: {table_name} | Load type: {load_type}")
    ensure_automerge()
    
    # Capture row count before load for validation
    incoming_count = df.count()
    print(f"  Incoming records: {incoming_count:,}")
    
    if load_type == "merge":
        if not key_expr:
            raise ValueError("key_expr is required for merge loads")
        
        if table_exists(table_name):
            print("  Performing MERGE (upsert) operation...")
            
            # Get pre-merge count
            existing_count = spark.table(table_name).count()
            print(f"  Existing records: {existing_count:,}")
            
            delta_table = DeltaTable.forName(spark, table_name)
            builder = delta_table.alias("t").merge(df.alias("s"), key_expr)
            builder = builder.whenMatchedUpdateAll().whenNotMatchedInsertAll()
            
            if delete_condition:
                print(f"  Delete condition enabled: {delete_condition}")
                builder = builder.whenNotMatchedBySourceDelete(condition=delete_condition)
            
            merge_metrics = builder.execute()
            
            # Validate post-merge
            final_count = spark.table(table_name).count()
            print(f"  Post-merge records: {final_count:,}")
            print(f"  Net change: {final_count - existing_count:+,}")
            print("  ✓ Merge completed successfully")
        else:
            print("  Table doesn't exist, creating new managed table...")
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            final_count = spark.table(table_name).count()
            print(f"  Table created with {final_count:,} records")
            print("  ✓ Table created successfully in metastore")
    
    elif load_type == "overwrite":
        print("  Performing OVERWRITE operation...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        final_count = spark.table(table_name).count()
        print(f"  Table now has {final_count:,} records")
        print("  ✓ Overwrite completed successfully")
    
    elif load_type == "append":
        print("  Performing APPEND operation...")
        existing_count = spark.table(table_name).count() if table_exists(table_name) else 0
        df.write.format("delta").mode("append").saveAsTable(table_name)
        final_count = spark.table(table_name).count()
        print(f"  Records added: {final_count - existing_count:,}")
        print("  ✓ Append completed successfully")
    
    print("\nSample of loaded data:")
    spark.table(table_name).show(5, truncate=False)
    print("\nSchema:")
    spark.table(table_name).printSchema()

# =============================================================================
# STEP 4: DATA QUALITY CHECKS
# =============================================================================

def run_data_quality_checks(table_name):
    """Run data quality checks on the candidates fact table"""
    print("\n" + "="*80)
    print("DATA QUALITY CHECKS")
    print("="*80)
    
    df = spark.sql(f"SELECT * FROM {table_name}")
    
    # Check 1: Row count
    row_count = df.count()
    print(f"\n✓ Total Rows: {row_count:,}")
    
    # Check 2: Unique candidates
    unique_candidates = df.select("candidate_id").distinct().count()
    print(f"✓ Unique Candidates: {unique_candidates:,}")
    
    # Check 3: Unique jobs
    unique_jobs = df.select("job_shortcode").distinct().count()
    print(f"✓ Unique Jobs: {unique_jobs:,}")
    
    # Check 4: Duplicate composite keys
    duplicate_keys = df.groupBy("candidate_job_key").count().filter(col("count") > 1).count()
    if duplicate_keys > 0:
        print(f"\n⚠ Warning: {duplicate_keys} duplicate candidate-job combinations found")
    else:
        print(f"\n✓ No duplicate candidate-job combinations")
    
    # Check 5: Null values in key fields
    print("\n✓ Null Value Check (Key Fields):")
    for column in ["candidate_id", "job_shortcode", "email", "stage"]:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"  - {column}: {null_count} nulls ({null_count/row_count*100:.1f}%)")
        else:
            print(f"  - {column}: No nulls ✓")
    
    # Check 6: Stage distribution
    print("\n✓ Candidates by Stage:")
    df.groupBy("stage").count().orderBy(col("count").desc()).show(truncate=False)
    
    # Check 7: Hired vs Not Hired
    print("\n✓ Hiring Status:")
    df.groupBy("is_hired").count().show()
    
    # Check 8: Disqualified count
    disqualified_count = df.filter(col("disqualified") == True).count()
    print(f"\n✓ Disqualified Candidates: {disqualified_count:,} ({disqualified_count/row_count*100:.1f}%)")
    
    # Check 9: Date range coverage
    print("\n✓ Date Range Coverage:")
    from pyspark.sql.functions import min as spark_min, max as spark_max
    date_stats = df.select(
        spark_min("updated_at").alias("min_update"),
        spark_max("updated_at").alias("max_update")
    ).collect()[0]
    
    print(f"  Earliest update: {date_stats['min_update']}")
    print(f"  Latest update: {date_stats['max_update']}")
    
    print("\n" + "="*80)
    print("DATA QUALITY CHECKS COMPLETE")
    print("="*80)

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("="*80)
    print("WORKABLE CANDIDATES ETL - INCREMENTAL LOADER")
    print("="*80)
    print(f"Execution started: {datetime.now()}\n")
    print_config()
    
    # If using overwrite mode, drop table to prevent schema conflicts
    if LOAD_TYPE == "overwrite" and table_exists(LAKEHOUSE_TABLE_NAME):
        print("\n" + "="*80)
        print("⚠ OVERWRITE MODE: Dropping table to prevent schema conflicts")
        print("="*80)
        spark.sql(f"DROP TABLE IF EXISTS {LAKEHOUSE_TABLE_NAME}")
        print(f"✓ Dropped {LAKEHOUSE_TABLE_NAME}")
        print("="*80 + "\n")
    
    # STEP 1: Extract candidates from API
    candidates_data = get_workable_candidates(
        API_TOKEN,
        updated_after=START_TIME_UNIX,
        updated_before=END_TIME_UNIX
    )
    
    if not candidates_data:
        print("\n⚠ No candidates data retrieved. Exiting.")
        return
    
    # STEP 1b: Write raw JSON
    raw_file_path = f"{LAKEHOUSE_RAW_DIR}/{START_DATE:%Y-%m-%d}_{datetime.now():%H%M%S}_candidates_raw.json"
    print(f"\nSaving raw JSON to: {raw_file_path}")
    write_raw_json_to_lakehouse(raw_file_path, json.dumps(candidates_data, indent=2))
    
    # STEP 2: Transform
    df_candidates = transform_candidates(candidates_data)
    
    if df_candidates is None:
        print("\n⚠ Transformation returned no data. Exiting.")
        return
    
    # STEP 3: Load to Lakehouse
    key_expr = (
        "(coalesce(t.candidate_id,'') = coalesce(s.candidate_id,'')) AND "
        "(coalesce(t.job_shortcode,'') = coalesce(s.job_shortcode,''))"
    )
    
    # Delete condition: remove records in the time window that are no longer present
    delete_condition = (
        f"t.updated_at >= CAST('{START_DATE}' AS TIMESTAMP) AND "
        f"t.updated_at <= CAST('{END_DATE}' AS TIMESTAMP)"
    )
    
    load_to_lakehouse(
        df_candidates,
        LAKEHOUSE_TABLE_NAME,
        load_type=LOAD_TYPE,
        key_expr=key_expr,
        delete_condition=delete_condition if LOAD_TYPE == "merge" else None
    )
    
    # STEP 4: Data Quality Checks
    try:
        run_data_quality_checks(LAKEHOUSE_TABLE_NAME)
    except Exception as e:
        print(f"Error running data quality checks: {e}")
    
    # Final summary
    print("\n" + "="*80)
    print("ETL COMPLETED")
    print("="*80)
    print(f"Completion time: {datetime.now()}")
    print(f"\nCandidates extracted: {len(candidates_data):,}")
    print(f"Records loaded: {df_candidates.count():,}")
    print(f"Target table: {LAKEHOUSE_TABLE_NAME}")
    print(f"Load type: {LOAD_TYPE}")
    
    print("\n" + "="*80)
    print("NEXT STEPS")
    print("="*80)
    print("1. Verify table appears in Power BI under 'dbo' schema")
    print("2. Create relationships with dim_workable_stages")
    print("3. Build candidate funnel visualizations")
    print("4. Schedule this notebook to run daily for incremental updates")
    print("="*80)

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
