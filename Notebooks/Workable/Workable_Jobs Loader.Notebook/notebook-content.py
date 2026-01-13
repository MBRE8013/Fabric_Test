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

# Fabric Notebook: Workable Jobs Dimension Loader
# Purpose: Load job requisition data from Workable API and create dimension table for Power BI
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Last Updated: 2025-11-17

# %%
# Setup and Configuration

import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, to_timestamp, explode_outer, coalesce, size as array_size
from datetime import datetime
import json
import time
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access

# Initialize Spark session
spark = SparkSession.builder.appName("WorkableJobsLoader").getOrCreate()

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
WORKABLE_SUBDOMAIN = "intellatriage"
WORKABLE_API_BASE = f"https://{WORKABLE_SUBDOMAIN}.workable.com/spi/v3"

# Target table name (will be created in default lakehouse)
TARGET_TABLE = "dim_workable_jobs"

# API endpoint
JOBS_ENDPOINT = f"{WORKABLE_API_BASE}/jobs"

# API pagination settings
LIMIT = 50  # Jobs per page (Workable max is 50)

# Rate limiting (Account tokens: 10 requests per 10 seconds)
RATE_LIMIT_DELAY = 1.2

# Raw Files path (Fabric Lakehouse)
LAKEHOUSE_RAW_DIR = "Files/raw/workable_jobs"

# %%
# Define API Request Function

def get_workable_jobs(api_token):
    """
    Fetch all jobs from Workable API with pagination
    
    Args:
        api_token (str): Workable API bearer token
    
    Returns:
        list: List of job dictionaries
    """
    headers = {
        'Authorization': f'Bearer {api_token}',
        'accept': 'application/json'
    }
    
    all_jobs = []
    page_count = 0
    since_id = None
    
    print(f"\n{'='*80}")
    print(f"EXTRACTING JOBS FROM WORKABLE")
    print(f"{'='*80}")
    
    try:
        while True:
            page_count += 1
            
            # Build query parameters
            params = {
                "limit": LIMIT
            }
            
            # Add pagination using since_id
            if since_id:
                params["since_id"] = since_id
            
            print(f"  Fetching page {page_count}..." + (f" (since_id: {since_id})" if since_id else ""))
            response = requests.get(JOBS_ENDPOINT, headers=headers, params=params, timeout=30)
            
            # Check for rate limiting
            if response.status_code == 429:
                reset_time = response.headers.get('X-Rate-Limit-Reset', 'unknown')
                print(f"  ⚠ Rate limit hit. Reset time: {reset_time}")
                print(f"  Waiting 15 seconds before retry...")
                time.sleep(15)
                continue
            
            # Check response status
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            jobs = data.get('jobs', [])
            if not jobs:
                print("  No more jobs found. Stopping pagination.")
                break
            
            all_jobs.extend(jobs)
            print(f"    Retrieved {len(jobs)} jobs (Total: {len(all_jobs)})")
            
            # Check for next page using paging info
            paging = data.get("paging", {})
            next_url = paging.get("next")
            
            if not next_url:
                print(f"  Last page reached (no next URL)")
                break
            
            # Extract since_id from next URL
            if "since_id=" in next_url:
                since_id = next_url.split("since_id=")[1].split("&")[0]
            else:
                print(f"  ⚠ Cannot extract since_id from next URL: {next_url}")
                break
            
            # Respect rate limits
            time.sleep(RATE_LIMIT_DELAY)
        
        print(f"✓ Successfully fetched {len(all_jobs)} jobs")
        return all_jobs
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching jobs: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text[:500]}")
        raise

# %%
# Fetch Jobs Data

# Fetch jobs from API
jobs_data = get_workable_jobs(WORKABLE_API_TOKEN)

# Display sample data
print("\n" + "="*80)
print("SAMPLE JOB DATA:")
print("="*80)
print(json.dumps(jobs_data[:2] if len(jobs_data) > 0 else [], indent=2))

# %%
# Transform Data for Dimension Table

def transform_jobs_data(jobs_list):
    """
    Transform raw jobs data into dimension table structure
    
    Args:
        jobs_list (list): Raw jobs data from API
    
    Returns:
        pd.DataFrame: Cleaned and structured DataFrame
    """
    if not jobs_list:
        print("⚠ Warning: No jobs data to transform")
        return pd.DataFrame()
    
    # Create DataFrame from jobs list
    df = pd.json_normalize(jobs_list)
    
    print(f"\n✓ Normalized {len(df)} jobs")
    print(f"Initial columns: {len(df.columns)}")
    
    # Add metadata columns
    df['etl_loaded_at'] = datetime.now()
    df['etl_source'] = 'workable_api'
    
    print(f"Columns after metadata: {', '.join(df.columns.tolist())}")
    
    return df

# %%
# Transform the data

# Transform the data
df_pandas = transform_jobs_data(jobs_data)

# Display the transformed data
print("\n" + "="*80)
print("TRANSFORMED JOBS DATA:")
print("="*80)
print(df_pandas.head(10))
print(f"\nShape: {df_pandas.shape}")

# %%
# Create Spark DataFrame with Proper Column Selection

if not df_pandas.empty:
    # Convert pandas DataFrame to Spark DataFrame
    df_spark_raw = spark.createDataFrame(df_pandas)
    
    print("\n" + "="*80)
    print("RAW SPARK DATAFRAME SCHEMA:")
    print("="*80)
    df_spark_raw.printSchema()
    
    # Select and rename columns for clean dimension table
    # Note: pandas json_normalize flattens nested fields with dots, so we use backticks
    df_spark = df_spark_raw.select(
        # Primary key
        col("id").alias("job_id"),
        col("shortcode").alias("job_shortcode"),
        
        # Job details
        col("title").alias("job_title"),
        col("full_title"),
        col("code").alias("job_code"),
        col("state").alias("job_state"),  # published, archived, draft, closed
        col("sample").cast("boolean").alias("is_sample"),
        col("confidential").cast("boolean").alias("is_confidential"),
        
        # Department info
        col("department").alias("department_name"),
        
        # Location info (flattened from nested structure)
        col("`location.location_str`").alias("location_string"),
        col("`location.country`").alias("country"),
        col("`location.country_code`").alias("country_code"),
        col("`location.region`").alias("region"),
        col("`location.region_code`").alias("region_code"),
        col("`location.city`").alias("city"),
        col("`location.zip_code`").alias("zip_code"),
        col("`location.telecommuting`").cast("boolean").alias("is_telecommuting"),
        col("`location.workplace_type`").alias("workplace_type"),  # remote, on_site, hybrid
        
        # Salary info (may be null for many jobs)
        col("`salary.salary_from`").cast("double").alias("salary_from"),
        col("`salary.salary_to`").cast("double").alias("salary_to"),
        col("`salary.salary_currency`").alias("salary_currency"),
        
        # URLs
        col("url").alias("job_url"),
        col("application_url"),
        col("shortlink"),
        
        # Timestamps
        to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("job_created_at"),
        
        # ETL metadata
        col("etl_loaded_at"),
        col("etl_source")
    )
    
    # Add derived columns
    df_spark = df_spark.withColumn(
        "is_active",
        col("job_state").isin(["published", "draft"])
    ).withColumn(
        "is_archived",
        col("job_state") == "archived"
    ).withColumn(
        "has_salary_range",
        (col("salary_from").isNotNull()) | (col("salary_to").isNotNull())
    ).withColumn(
        "days_since_created",
        ((current_timestamp().cast("long") - col("job_created_at").cast("long")) / 86400).cast("int")
    )
    
    # Show schema and data
    print("\n" + "="*80)
    print("FINAL SPARK DATAFRAME SCHEMA:")
    print("="*80)
    df_spark.printSchema()
    
    print("\n" + "="*80)
    print("FINAL SPARK DATAFRAME PREVIEW:")
    print("="*80)
    df_spark.show(10, truncate=False)
    
    print(f"\n✓ Created Spark DataFrame with {df_spark.count()} rows")
else:
    print("⚠ Warning: Empty DataFrame - no data to create Spark DataFrame")
    df_spark = None

# %%
# Write to Delta Table (Managed Table)

if df_spark is not None and not df_pandas.empty:
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

if df_spark is not None:
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
        
        # Show jobs by state
        print("\nJobs by State:")
        verify_df.groupBy("job_state").count().orderBy("count", ascending=False).show()
        
        # Show jobs by department
        print("\nTop 10 Departments:")
        verify_df.groupBy("department_name").count().orderBy("count", ascending=False).show(10)
        
        # Show jobs by workplace type
        print("\nJobs by Workplace Type:")
        verify_df.groupBy("workplace_type").count().show()
        
        # Show active vs archived
        print("\nActive vs Archived:")
        verify_df.groupBy("is_active").count().show()
        
        # Show jobs with salary ranges
        print("\nJobs with Salary Information:")
        verify_df.groupBy("has_salary_range").count().show()
        
    except Exception as e:
        print(f"✗ Error verifying table: {str(e)}")

# %%
# Data Quality Checks

def run_data_quality_checks(table_name):
    """
    Run data quality checks on the jobs dimension table
    """
    print("\n" + "="*80)
    print("DATA QUALITY CHECKS")
    print("="*80)
    
    df = spark.sql(f"SELECT * FROM {table_name}")
    
    # Check 1: Row count
    row_count = df.count()
    print(f"\n✓ Total Rows: {row_count}")
    
    # Check 2: Null values in key fields
    print("\n✓ Null Value Check (Key Fields):")
    for column in ["job_id", "job_shortcode", "job_title", "job_state"]:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"  - {column}: {null_count} nulls ({null_count/row_count*100:.1f}%)")
        else:
            print(f"  - {column}: No nulls ✓")
    
    # Check 3: Duplicate shortcodes
    duplicate_count = df.groupBy("job_shortcode").count().filter(col("count") > 1).count()
    if duplicate_count > 0:
        print(f"\n⚠ Warning: {duplicate_count} duplicate job shortcodes found")
        print("Showing duplicates:")
        df.groupBy("job_shortcode").count().filter(col("count") > 1).show()
    else:
        print(f"\n✓ No duplicate job shortcodes")
    
    # Check 4: State distribution
    print("\n✓ Job State Distribution:")
    state_dist = df.groupBy("job_state").count().orderBy("count", ascending=False)
    state_dist.show()
    
    # Check 5: Active job count
    active_count = df.filter(col("is_active") == True).count()
    print(f"\n✓ Active Jobs: {active_count} ({active_count/row_count*100:.1f}%)")
    
    # Check 6: Remote vs On-site
    print("\n✓ Workplace Type Distribution:")
    df.groupBy("workplace_type").count().orderBy("count", ascending=False).show()
    
    # Check 7: Salary information completeness
    with_salary = df.filter(col("has_salary_range") == True).count()
    print(f"\n✓ Jobs with Salary Info: {with_salary} ({with_salary/row_count*100:.1f}%)")
    
    print("\n" + "="*80)
    print("DATA QUALITY CHECKS COMPLETE")
    print("="*80)

# Run the checks
if df_spark is not None:
    try:
        run_data_quality_checks(TARGET_TABLE)
    except Exception as e:
        print(f"Error running data quality checks: {e}")

# %%
# Save Raw JSON

if jobs_data:
    raw_file_path = f"{LAKEHOUSE_RAW_DIR}/{datetime.now():%Y%m%d_%H%M%S}_jobs_raw.json"
    print(f"\nSaving raw JSON to: {raw_file_path}")
    
    try:
        from notebookutils import mssparkutils
        try:
            mssparkutils.fs.mkdirs(LAKEHOUSE_RAW_DIR)
        except Exception:
            pass
        mssparkutils.fs.put(raw_file_path, json.dumps(jobs_data, indent=2), overwrite=True)
        print(f"  ✓ Raw JSON written to Lakehouse: {raw_file_path}")
    except Exception as e:
        import os
        os.makedirs(LAKEHOUSE_RAW_DIR.replace('Files/', ''), exist_ok=True)
        with open(raw_file_path.replace('Files/', ''), 'w', encoding='utf-8') as f:
            f.write(json.dumps(jobs_data, indent=2))
        print(f"  ⚠ Wrote raw JSON to local disk: {raw_file_path} | {e}")

# %%
# Completion Summary

print("\n" + "="*80)
print("WORKABLE JOBS LOADER - COMPLETION SUMMARY")
print("="*80)
print(f"✓ API Endpoint: {JOBS_ENDPOINT}")
print(f"✓ Target Table: {TARGET_TABLE}")
print(f"✓ Rows Loaded: {df_spark.count() if df_spark is not None else 0}")
print(f"✓ Load Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\nNext Steps:")
print("1. Verify table appears in Power BI under 'dbo' schema")
print("2. Create relationship: workable_candidates[job_shortcode] → dim_workable_jobs[job_shortcode]")
print("3. Use job attributes for filtering and grouping in reports")
print("4. Schedule this notebook to run weekly (jobs don't change frequently)")
print("5. Consider adding department hierarchy if needed")
print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
