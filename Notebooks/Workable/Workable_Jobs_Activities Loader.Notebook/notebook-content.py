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

# Fabric Notebook: Workable Job Activities Loader
# Purpose: Load job application activities (applications) from Workable API and create fact table for Power BI
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Last Updated: 2025-12-15

# %%
# Setup and Configuration

import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, current_timestamp, to_timestamp, lit, 
    explode_outer, coalesce, when, concat, sha2, min, max
)
from datetime import datetime, timedelta
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import deque
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access

# Initialize Spark session
spark = SparkSession.builder.appName("WorkableActivitiesLoader").getOrCreate()

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

# Source and Target Tables
SOURCE_TABLE = "dim_workable_jobs"  # Jobs dimension table
DATABASE = os.getenv("FABRIC_DB", "")
LAKEHOUSE_TABLE_NAME = "fact_workable_activities" if not DATABASE else f"{DATABASE}.fact_workable_activities"
TARGET_TABLE = LAKEHOUSE_TABLE_NAME

# =============================================================================
# LOAD STRATEGY CONFIGURATION
# =============================================================================

# TEST MODE: Set to True for initial testing with shorter timeframes
TEST_MODE = False  # Set to True for testing, False for production runs

# TEST SINGLE JOB: Set to a job_shortcode to test historical backfill with just one job
TEST_SINGLE_JOB = None  # Example: "BEB8EDA1AA" or None for all jobs

# Toggle for historical vs incremental load
HISTORICAL_BACKFILL = False  # Set to True for one-time backfill, False for daily incremental

if TEST_MODE:
    # TESTING MODE: Short timeframes for validation
    print("⚠ RUNNING IN TEST MODE - SHORT TIMEFRAMES")
    if HISTORICAL_BACKFILL:
        # Test historical: Last 3 months only
        LOAD_TYPE = "overwrite"
        START_DATE = (datetime.now() - timedelta(days=90)).replace(hour=0, minute=0, second=0, microsecond=0)
        END_DATE = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
        print(f"  Test Historical Backfill: Last 90 days")
    else:
        # Test incremental: Last 3 days (standard)
        LOAD_TYPE = os.getenv("LOAD_TYPE", "merge")
        DAYS_BACK = 3
        NOW = datetime.now()
        START_DATE = (NOW - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0, second=0, microsecond=0)
        END_DATE = NOW.replace(hour=23, minute=59, second=59, microsecond=0)
        print(f"  Test Incremental: Last {DAYS_BACK} days")
elif HISTORICAL_BACKFILL:
    # PRODUCTION HISTORICAL BACKFILL: From Jan 1, 2024 through today
    print("⚠ RUNNING IN HISTORICAL BACKFILL MODE - JAN 2024 TO PRESENT")
    LOAD_TYPE = "overwrite"
    START_DATE = datetime(2024, 1, 1, 0, 0, 0)
    END_DATE = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
else:
    # PRODUCTION INCREMENTAL LOAD: Last 3 days
    LOAD_TYPE = os.getenv("LOAD_TYPE", "merge")
    DAYS_BACK = int(os.getenv("DAYS_BACK", "3"))
    NOW = datetime.now()
    START_DATE = (NOW - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0, second=0, microsecond=0)
    END_DATE = NOW.replace(hour=23, minute=59, second=59, microsecond=0)

# API pagination settings
LIMIT = 100  # Activities per page (Workable max is 100)

# Rate limiting (Account tokens: 10 requests per 10 seconds)
RATE_LIMIT_DELAY = 1.2

# Parallel processing configuration
MAX_WORKERS = 6  # Reduced from 8 - more conservative for pagination
RATE_LIMIT_MAX_REQUESTS = 9  # Stay under 10 req/10s limit
RATE_LIMIT_TIME_WINDOW = 10  # seconds

# Raw Files path (Fabric Lakehouse)
LAKEHOUSE_RAW_DIR = "Files/raw/workable_activities"

# %%
# Display Load Configuration

print(f"\n{'='*80}")
if TEST_MODE:
    print(f"⚠⚠⚠ TEST MODE ENABLED ⚠⚠⚠")
    print(f"{'='*80}")
if TEST_SINGLE_JOB:
    print(f"⚠⚠⚠ SINGLE JOB TEST: {TEST_SINGLE_JOB} ⚠⚠⚠")
    print(f"{'='*80}")
if HISTORICAL_BACKFILL:
    print(f"HISTORICAL BACKFILL MODE")
else:
    print(f"INCREMENTAL LOAD MODE")
print(f"{'='*80}")
print(f"Test Mode: {TEST_MODE}")
print(f"Test Single Job: {TEST_SINGLE_JOB or 'Disabled'}")
print(f"Load Type: {LOAD_TYPE}")
print(f"Date Range: {START_DATE.strftime('%Y-%m-%d %H:%M:%S')} to {END_DATE.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Duration: {(END_DATE - START_DATE).days} days")
if not HISTORICAL_BACKFILL:
    print(f"Days Back: {DAYS_BACK}")
print(f"Target Table: {TARGET_TABLE}")
print(f"{'='*80}\n")

# Convert to Unix timestamps (milliseconds) for API
start_timestamp = int(START_DATE.timestamp() * 1000)
end_timestamp = int(END_DATE.timestamp() * 1000)

# %%
# Rate Limiter Class for Parallel Processing

class RateLimiter:
    """
    Thread-safe rate limiter for managing API request throttling
    Ensures we don't exceed Workable's rate limits when using parallel processing
    """
    def __init__(self, max_requests=9, time_window=10):
        """
        Args:
            max_requests (int): Maximum requests allowed in time window
            time_window (int): Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = threading.Lock()
        self.wait_count = 0  # Track how often we're waiting
        self.total_wait_time = 0  # Track total wait time
    
    def wait_if_needed(self):
        """
        Wait if necessary to stay within rate limits
        Thread-safe implementation using deque and lock
        """
        with self.lock:
            now = time.time()
            
            # Remove requests older than time_window
            while self.requests and self.requests[0] < now - self.time_window:
                self.requests.popleft()
            
            # If at limit, wait until oldest request expires
            if len(self.requests) >= self.max_requests:
                sleep_time = self.time_window - (now - self.requests[0]) + 0.05  # Reduced buffer to 50ms
                if sleep_time > 0:
                    self.wait_count += 1
                    self.total_wait_time += sleep_time
                    time.sleep(sleep_time)
                    now = time.time()
                # Remove the oldest request
                self.requests.popleft()
            
            # Record this request
            self.requests.append(now)
    
    def get_stats(self):
        """Return statistics about rate limiting"""
        return {
            'wait_count': self.wait_count,
            'total_wait_time': self.total_wait_time,
            'avg_wait_time': self.total_wait_time / self.wait_count if self.wait_count > 0 else 0
        }

# Initialize global rate limiter
rate_limiter = RateLimiter(
    max_requests=RATE_LIMIT_MAX_REQUESTS, 
    time_window=RATE_LIMIT_TIME_WINDOW
)

# %%
# Get Job Shortcodes from Dimension Table

def get_job_shortcodes():
    """
    Retrieve list of job shortcodes from the jobs dimension table
    Apply smart filtering for incremental loads
    Can be overridden by TEST_SINGLE_JOB for testing
    
    Returns:
        list: List of job shortcode strings
    """
    print(f"\n{'='*80}")
    print(f"RETRIEVING JOB SHORTCODES FROM {SOURCE_TABLE}")
    print(f"{'='*80}")
    
    # Override with single job if TEST_SINGLE_JOB is set
    if TEST_SINGLE_JOB:
        print(f"⚠ TEST_SINGLE_JOB MODE: Using only job {TEST_SINGLE_JOB}")
        
        # Verify the job exists in the dimension table
        try:
            job_check = spark.sql(f"""
                SELECT job_shortcode, job_title, job_state 
                FROM {SOURCE_TABLE} 
                WHERE job_shortcode = '{TEST_SINGLE_JOB}'
            """)
            
            if job_check.count() == 0:
                raise ValueError(f"Job shortcode '{TEST_SINGLE_JOB}' not found in {SOURCE_TABLE}")
            
            job_info = job_check.collect()[0]
            print(f"✓ Found test job: {job_info.job_title} (State: {job_info.job_state})")
            print(f"✓ Using single job for testing\n")
            
            return [TEST_SINGLE_JOB]
            
        except Exception as e:
            print(f"✗ Error verifying test job: {str(e)}")
            raise
    
    # Normal job retrieval logic
    try:
        if HISTORICAL_BACKFILL:
            # Historical: Get ALL jobs
            print("HISTORICAL MODE: Loading all jobs")
            query = f"""
                SELECT DISTINCT job_shortcode 
                FROM {SOURCE_TABLE} 
                WHERE job_shortcode IS NOT NULL
                ORDER BY job_shortcode
            """
        else:
            # Incremental: Smart filtering
            print("INCREMENTAL MODE: Filtering to active/recent jobs")
            query = f"""
                SELECT DISTINCT job_shortcode 
                FROM {SOURCE_TABLE} 
                WHERE job_shortcode IS NOT NULL
                AND (
                    job_state = 'published'  -- Currently published jobs
                    OR days_since_created <= 90  -- Any job created in last 90 days (catches recently closed)
                )
                ORDER BY job_shortcode
            """
        
        jobs_df = spark.sql(query)
        
        job_count = jobs_df.count()
        print(f"✓ Found {job_count} jobs matching criteria")
        
        # Show breakdown for incremental mode
        if not HISTORICAL_BACKFILL:
            breakdown = spark.sql(f"""
                SELECT 
                    job_state,
                    COUNT(*) as job_count
                FROM {SOURCE_TABLE} 
                WHERE job_shortcode IS NOT NULL
                AND (
                    job_state = 'published'
                    OR days_since_created <= 90
                )
                GROUP BY job_state
                ORDER BY job_count DESC
            """)
            print("\nJob State Breakdown:")
            breakdown.show()
        
        # Convert to Python list
        shortcodes = [row.job_shortcode for row in jobs_df.collect()]
        
        print(f"✓ Retrieved {len(shortcodes)} unique job shortcodes")
        print(f"Sample shortcodes: {shortcodes[:5]}")
        
        return shortcodes
        
    except Exception as e:
        print(f"✗ Error retrieving job shortcodes: {str(e)}")
        raise

# %%
# Define API Request Function

def get_job_activities(api_token, job_shortcode, since_date_ms, until_date_ms):
    """
    Fetch activities for a specific job from Workable API with pagination
    Uses global rate_limiter for thread-safe rate limiting
    
    Args:
        api_token (str): Workable API bearer token
        job_shortcode (str): Job shortcode identifier
        since_date_ms (int): Start date in Unix timestamp (milliseconds)
        until_date_ms (int): End date in Unix timestamp (milliseconds)
    
    Returns:
        list: List of activity dictionaries filtered to only 'applied' actions
    """
    headers = {
        'Authorization': f'Bearer {api_token}',
        'accept': 'application/json'
    }
    
    activities_endpoint = f"{WORKABLE_API_BASE}/jobs/{job_shortcode}/activities"
    
    all_activities = []
    page_count = 0
    since_id = None
    
    try:
        while True:
            page_count += 1
            
            # Build query parameters
            # NOTE: We do NOT use since_date/until_date because they don't filter by created_at
            # We filter client-side after retrieving the data
            params = {
                "limit": LIMIT
            }
            
            # Add pagination using since_id
            if since_id:
                params["since_id"] = since_id
            
            # Wait if needed to respect rate limits (thread-safe)
            rate_limiter.wait_if_needed()
            
            response = requests.get(activities_endpoint, headers=headers, params=params, timeout=30)
            
            # Check for rate limiting
            if response.status_code == 429:
                reset_time = response.headers.get('X-Rate-Limit-Reset', 'unknown')
                print(f"    ⚠ Rate limit hit for {job_shortcode}. Reset time: {reset_time}")
                print(f"    Waiting 15 seconds before retry...")
                time.sleep(15)
                continue
            
            # Check for 404 (job not found) - this is OK, just skip
            if response.status_code == 404:
                # Silent skip for 404s - expected for some jobs
                break
            
            # Check response status
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            activities = data.get('activities', [])
            if not activities:
                break
            
            # Filter to only 'applied' actions
            applied_activities = [act for act in activities if act.get('action') == 'applied']
            
            if applied_activities:
                all_activities.extend(applied_activities)
            
            # Check for next page using paging info
            paging = data.get("paging", {})
            next_url = paging.get("next")
            
            if not next_url:
                break
            
            # Extract since_id from next URL
            if "since_id=" in next_url:
                since_id = next_url.split("since_id=")[1].split("&")[0]
            else:
                break
        
        return all_activities
        
    except requests.exceptions.RequestException as e:
        print(f"    ✗ Error fetching activities for {job_shortcode}: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"    Response status: {e.response.status_code}")
        return []

# %%
# Fetch Activities for All Jobs (with Parallel Processing)

def fetch_all_activities_parallel(api_token, job_shortcodes, since_ms, until_ms, max_workers=8):
    """
    Fetch activities for all jobs using parallel processing with rate limiting
    
    Args:
        api_token (str): Workable API bearer token
        job_shortcodes (list): List of job shortcode strings
        since_ms (int): Start date in Unix timestamp (milliseconds)
        until_ms (int): End date in Unix timestamp (milliseconds)
        max_workers (int): Number of parallel threads
    
    Returns:
        list: List of all activity dictionaries with job_shortcode added
    """
    print(f"\n{'='*80}")
    print(f"EXTRACTING ACTIVITIES FROM WORKABLE (PARALLEL MODE)")
    print(f"{'='*80}")
    print(f"Processing {len(job_shortcodes)} jobs...")
    print(f"Max parallel workers: {max_workers}")
    print(f"Rate limit: {RATE_LIMIT_MAX_REQUESTS} requests per {RATE_LIMIT_TIME_WINDOW} seconds")
    print(f"{'='*80}\n")
    
    all_activities = []
    jobs_processed = 0
    jobs_with_activities = 0
    jobs_with_errors = 0
    lock = threading.Lock()  # Thread-safe counter updates
    
    # Progress tracking
    start_time = time.time()
    
    def fetch_job_activities(shortcode):
        """Wrapper function for parallel execution"""
        nonlocal jobs_processed, jobs_with_activities, jobs_with_errors
        
        try:
            activities = get_job_activities(api_token, shortcode, since_ms, until_ms)
            
            if activities:
                # Add job_shortcode to each activity
                for activity in activities:
                    activity['job_shortcode'] = shortcode
                
                with lock:
                    jobs_with_activities += 1
                
                return (shortcode, activities, None)
            else:
                return (shortcode, [], None)
                
        except Exception as e:
            with lock:
                jobs_with_errors += 1
            return (shortcode, [], str(e))
        finally:
            with lock:
                jobs_processed += 1
    
    # Execute parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        future_to_shortcode = {
            executor.submit(fetch_job_activities, shortcode): shortcode 
            for shortcode in job_shortcodes
        }
        
        # Process completed futures
        for future in as_completed(future_to_shortcode):
            shortcode = future_to_shortcode[future]
            
            try:
                shortcode, activities, error = future.result()
                
                if error:
                    print(f"[{jobs_processed}/{len(job_shortcodes)}] ✗ {shortcode}: Error - {error}")
                elif activities:
                    all_activities.extend(activities)
                    print(f"[{jobs_processed}/{len(job_shortcodes)}] ✓ {shortcode}: {len(activities)} activities (Total: {len(all_activities)})")
                else:
                    # Silent success for jobs with no activities
                    pass
                
                # Progress update every 10 jobs
                if jobs_processed % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = jobs_processed / elapsed
                    remaining = len(job_shortcodes) - jobs_processed
                    eta_seconds = remaining / rate if rate > 0 else 0
                    
                    print(f"\n{'─'*80}")
                    print(f"Progress: {jobs_processed}/{len(job_shortcodes)} jobs ({jobs_processed/len(job_shortcodes)*100:.1f}%)")
                    print(f"Activities collected: {len(all_activities)}")
                    print(f"Jobs with activities: {jobs_with_activities}")
                    print(f"Processing rate: {rate:.2f} jobs/sec ({rate * 60:.1f} jobs/min)")
                    print(f"Estimated time remaining: {eta_seconds/60:.1f} minutes")
                    print(f"{'─'*80}\n")
                    
            except Exception as e:
                print(f"[{jobs_processed}/{len(job_shortcodes)}] ✗ {shortcode}: Unexpected error - {str(e)}")
    
    # Final summary
    elapsed_total = time.time() - start_time
    rate_stats = rate_limiter.get_stats()
    
    print(f"\n{'='*80}")
    print(f"EXTRACTION COMPLETE")
    print(f"{'='*80}")
    print(f"✓ Jobs processed: {jobs_processed}")
    print(f"✓ Jobs with activities: {jobs_with_activities}")
    print(f"✓ Jobs with errors: {jobs_with_errors}")
    print(f"✓ Total 'applied' activities: {len(all_activities)}")
    print(f"✓ Total time: {elapsed_total/60:.1f} minutes")
    print(f"✓ Average rate: {jobs_processed/elapsed_total:.1f} jobs/second")
    print(f"\nRate Limiter Statistics:")
    print(f"  - Times waited: {rate_stats['wait_count']}")
    print(f"  - Total wait time: {rate_stats['total_wait_time']:.1f} seconds")
    print(f"  - Avg wait per block: {rate_stats['avg_wait_time']:.2f} seconds")
    print(f"  - Efficiency: {((elapsed_total - rate_stats['total_wait_time']) / elapsed_total * 100):.1f}% (non-wait time)")
    print(f"{'='*80}\n")
    
    return all_activities

# %%
# Get job shortcodes and fetch activities

job_shortcodes = get_job_shortcodes()
activities_data = fetch_all_activities_parallel(
    WORKABLE_API_TOKEN, 
    job_shortcodes, 
    start_timestamp, 
    end_timestamp,
    max_workers=MAX_WORKERS
)

# Display sample data
if activities_data:
    print("\n" + "="*80)
    print("SAMPLE ACTIVITY DATA:")
    print("="*80)
    print(json.dumps(activities_data[:2], indent=2))
else:
    print("\n⚠ Warning: No activities data retrieved")

# %%
# Transform Data for Fact Table

def transform_activities_data(activities_list):
    """
    Transform raw activities data into fact table structure
    
    Args:
        activities_list (list): Raw activities data from API
    
    Returns:
        pd.DataFrame: Cleaned and structured DataFrame
    """
    if not activities_list:
        print("⚠ Warning: No activities data to transform")
        return pd.DataFrame()
    
    # Create DataFrame from activities list
    df = pd.json_normalize(activities_list)
    
    print(f"\n✓ Normalized {len(df)} activities")
    print(f"Initial columns: {len(df.columns)}")
    
    # Add metadata columns
    df['etl_loaded_at'] = datetime.now()
    df['etl_source'] = 'workable_api'
    df['etl_load_type'] = LOAD_TYPE
    df['etl_historical_backfill'] = HISTORICAL_BACKFILL
    
    print(f"Columns after metadata: {', '.join(df.columns.tolist())}")
    
    return df

# %%
# Transform the data

df_pandas = transform_activities_data(activities_data)

# Display the transformed data
if not df_pandas.empty:
    print("\n" + "="*80)
    print("TRANSFORMED ACTIVITIES DATA:")
    print("="*80)
    print(df_pandas.head(10))
    print(f"\nShape: {df_pandas.shape}")
    print(f"\nColumns: {df_pandas.columns.tolist()}")
else:
    print("\n⚠ Warning: No data to display")

# %%
# Create Spark DataFrame with Proper Column Selection

if not df_pandas.empty:
    # Convert pandas DataFrame to Spark DataFrame
    df_spark_raw = spark.createDataFrame(df_pandas)
    
    print("\n" + "="*80)
    print("RAW SPARK DATAFRAME SCHEMA:")
    print("="*80)
    df_spark_raw.printSchema()
    
    # Select and rename columns for clean fact table
    # Note: Some fields like member.id may not exist in all API responses
    # We'll check for their presence and handle accordingly
    
    available_columns = df_spark_raw.columns
    has_member_fields = 'member.id' in available_columns
    
    # Build column list dynamically based on what's available
    select_columns = [
        # Composite key columns
        col("id").alias("activity_id"),
        col("job_shortcode"),
        
        # Activity details
        col("action").alias("activity_action"),  # Should always be 'applied'
        # Note: activity_body is always NULL for 'applied' actions (only used for comments/messages)
        
        # Timestamps
        to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("activity_created_at"),
    ]
    
    # Add member info if present (often null for direct applications)
    if has_member_fields:
        select_columns.extend([
            col("`member.id`").alias("member_id"),
            col("`member.name`").alias("member_name"),
        ])
    else:
        select_columns.extend([
            lit(None).cast("string").alias("member_id"),
            lit(None).cast("string").alias("member_name"),
        ])
    
    # Add remaining columns
    select_columns.extend([
        # Candidate info
        col("`candidate.id`").alias("candidate_id"),
        col("`candidate.name`").alias("candidate_name"),
        
        # Stage info (recruiting stage - comes as simple string, not nested object)
        col("stage_name"),
        
        # ETL metadata
        col("etl_loaded_at"),
        col("etl_source"),
        col("etl_load_type"),
        col("etl_historical_backfill")
    ])
    
    df_spark = df_spark_raw.select(*select_columns)
    
    # Add derived columns
    df_spark = df_spark.withColumn(
        "activity_date",
        col("activity_created_at").cast("date")
    ).withColumn(
        "activity_year",
        col("activity_created_at").cast("date").substr(1, 4).cast("int")
    ).withColumn(
        "activity_month",
        col("activity_created_at").cast("date").substr(6, 2).cast("int")
    ).withColumn(
        "activity_quarter",
        when(col("activity_month").isin(1, 2, 3), 1)
        .when(col("activity_month").isin(4, 5, 6), 2)
        .when(col("activity_month").isin(7, 8, 9), 3)
        .otherwise(4)
    ).withColumn(
        "is_recent",
        col("activity_created_at") >= lit(datetime.now() - timedelta(days=30))
    ).withColumn(
        # Create a unique key for merge operations
        "merge_key",
        sha2(concat(col("activity_id"), col("job_shortcode")), 256)
    )
    
    # CRITICAL: Filter to only activities created within our date range
    # The API's since_date/until_date don't filter by created_at, so we must do it here
    print(f"\n⚠ IMPORTANT: Filtering activities by created_at date range")
    print(f"  Requested range: {START_DATE} to {END_DATE}")
    rows_before_filter = df_spark.count()
    print(f"  Rows before date filter: {rows_before_filter:,}")
    
    df_spark = df_spark.filter(
        (col("activity_created_at") >= lit(START_DATE)) & 
        (col("activity_created_at") <= lit(END_DATE))
    )
    
    rows_after_filter = df_spark.count()
    rows_filtered_out = rows_before_filter - rows_after_filter
    print(f"  Rows after date filter: {rows_after_filter:,}")
    print(f"  Rows filtered out: {rows_filtered_out:,} ({rows_filtered_out/rows_before_filter*100:.1f}%)")
    print(f"  ✓ Only keeping activities created in specified date range\n")
    
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
# Write to Delta Table with Merge Logic

if df_spark is not None and not df_pandas.empty:
    try:
        from delta.tables import DeltaTable
        
        # Check if table exists
        table_exists = spark.catalog.tableExists(TARGET_TABLE)
        
        if LOAD_TYPE == "overwrite" or not table_exists:
            print(f"\n{'='*80}")
            print(f"WRITING TO DELTA TABLE: {TARGET_TABLE}")
            print(f"Mode: OVERWRITE")
            print(f"{'='*80}")
            
            # Overwrite mode - used for historical backfill or if table doesn't exist
            df_spark.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(TARGET_TABLE)
            
            print(f"✓ Successfully created/overwritten table: {TARGET_TABLE}")
            print(f"  - Mode: overwrite")
            print(f"  - Format: Delta")
            print(f"  - Rows: {df_spark.count()}")
            
        else:
            print(f"\n{'='*80}")
            print(f"MERGING TO DELTA TABLE: {TARGET_TABLE}")
            print(f"Mode: MERGE (Upsert)")
            print(f"{'='*80}")
            
            # Merge mode - used for incremental loads
            delta_table = DeltaTable.forName(spark, TARGET_TABLE)
            
            # Perform merge operation
            # Match on activity_id and job_shortcode
            merge_result = delta_table.alias("target").merge(
                df_spark.alias("source"),
                "target.activity_id = source.activity_id AND target.job_shortcode = source.job_shortcode"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            print(f"✓ Successfully merged data into table: {TARGET_TABLE}")
            print(f"  - Mode: merge (upsert)")
            print(f"  - Format: Delta")
            print(f"  - Source rows: {df_spark.count()}")
            
            # Show merge statistics if available
            history = delta_table.history(1).select("operationMetrics").collect()
            if history and history[0].operationMetrics:
                metrics = history[0].operationMetrics
                print(f"  - Rows inserted: {metrics.get('numTargetRowsInserted', 'N/A')}")
                print(f"  - Rows updated: {metrics.get('numTargetRowsUpdated', 'N/A')}")
        
    except Exception as e:
        print(f"\n✗ Error writing to Delta table: {str(e)}")
        raise
else:
    print("⚠ Skipping table write - no data available")

# %%
# Verify Table Creation/Update

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
        
        # Show activities by date
        print("\nTop 10 Days by Activity Count:")
        verify_df.groupBy("activity_date").count() \
            .orderBy(col("activity_date").desc()) \
            .show(10)
        
        # Show activities by job
        print("\nTop 10 Jobs by Application Count:")
        verify_df.groupBy("job_shortcode").count() \
            .orderBy(col("count").desc()) \
            .show(10)
        
        # Show activities by year/month
        print("\nApplications by Year-Month:")
        verify_df.groupBy("activity_year", "activity_month").count() \
            .orderBy("activity_year", "activity_month") \
            .show(24)
        
        # Show recent vs older activities
        print("\nRecent vs Historical:")
        verify_df.groupBy("is_recent").count().show()
        
        # Date range coverage
        print("\nDate Range Coverage:")
        verify_df.select(
            col("activity_date").alias("date")
        ).agg(
            {"date": "min", "date": "max"}
        ).show()
        
    except Exception as e:
        print(f"✗ Error verifying table: {str(e)}")

# %%
# Data Quality Checks

def run_data_quality_checks(table_name):
    """
    Run data quality checks on the activities fact table
    """
    print("\n" + "="*80)
    print("DATA QUALITY CHECKS")
    print("="*80)
    
    df = spark.sql(f"SELECT * FROM {table_name}")
    
    # Check 1: Row count
    row_count = df.count()
    print(f"\n✓ Total Rows: {row_count:,}")
    
    # Check 2: Null values in key fields
    print("\n✓ Null Value Check (Key Fields):")
    for column in ["activity_id", "job_shortcode", "activity_action", "activity_created_at"]:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"  - {column}: {null_count} nulls ({null_count/row_count*100:.1f}%)")
        else:
            print(f"  - {column}: No nulls ✓")
    
    # Check 3: Verify all actions are 'applied'
    non_applied = df.filter(col("activity_action") != "applied").count()
    if non_applied > 0:
        print(f"\n⚠ Warning: {non_applied} activities with action != 'applied'")
        df.filter(col("activity_action") != "applied").groupBy("activity_action").count().show()
    else:
        print(f"\n✓ All activities have action = 'applied'")
    
    # Check 4: Duplicate check
    duplicate_count = df.groupBy("activity_id", "job_shortcode").count() \
        .filter(col("count") > 1).count()
    if duplicate_count > 0:
        print(f"\n⚠ Warning: {duplicate_count} duplicate activity records found")
    else:
        print(f"\n✓ No duplicate activity records")
    
    # Check 5: Date range verification
    print("\n✓ Date Range:")
    date_stats = df.select(
        min("activity_created_at").alias("min_date"),
        max("activity_created_at").alias("max_date")
    ).collect()[0]
    print(f"  - Earliest: {date_stats['min_date']}")
    print(f"  - Latest: {date_stats['max_date']}")
    
    # Check 6: Applications per job distribution
    print("\n✓ Applications per Job Statistics:")
    apps_per_job = df.groupBy("job_shortcode").count()
    apps_per_job.select("count").summary("count", "mean", "stddev", "min", "max").show()
    
    # Check 7: Candidate information completeness
    with_candidate = df.filter(col("candidate_id").isNotNull()).count()
    print(f"\n✓ Activities with Candidate Info: {with_candidate:,} ({with_candidate/row_count*100:.1f}%)")
    
    # Check 8: Recent activity check (last 7 days)
    last_7_days = df.filter(col("activity_created_at") >= lit(datetime.now() - timedelta(days=7))).count()
    print(f"\n✓ Activities in Last 7 Days: {last_7_days:,}")
    
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

if activities_data:
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    load_mode = 'backfill' if HISTORICAL_BACKFILL else 'incremental'
    raw_file_path = f"{LAKEHOUSE_RAW_DIR}/{timestamp_str}_activities_raw_{load_mode}.json"
    print(f"\nSaving raw JSON to: {raw_file_path}")
    
    try:
        from notebookutils import mssparkutils
        try:
            mssparkutils.fs.mkdirs(LAKEHOUSE_RAW_DIR)
        except Exception:
            pass
        mssparkutils.fs.put(raw_file_path, json.dumps(activities_data, indent=2), overwrite=True)
        print(f"  ✓ Raw JSON written to Lakehouse: {raw_file_path}")
    except Exception as e:
        import os
        os.makedirs(LAKEHOUSE_RAW_DIR.replace('Files/', ''), exist_ok=True)
        with open(raw_file_path.replace('Files/', ''), 'w', encoding='utf-8') as f:
            f.write(json.dumps(activities_data, indent=2))
        print(f"  ⚠ Wrote raw JSON to local disk: {raw_file_path} | {e}")

# %%
# Completion Summary

print("\n" + "="*80)
print("WORKABLE ACTIVITIES LOADER - COMPLETION SUMMARY")
print("="*80)
if TEST_MODE:
    print("⚠⚠⚠ TEST MODE WAS ENABLED ⚠⚠⚠")
if TEST_SINGLE_JOB:
    print(f"⚠⚠⚠ SINGLE JOB TEST MODE: {TEST_SINGLE_JOB} ⚠⚠⚠")
print(f"✓ Load Mode: {'HISTORICAL BACKFILL' if HISTORICAL_BACKFILL else 'INCREMENTAL'}")
print(f"✓ Test Mode: {TEST_MODE}")
print(f"✓ Test Single Job: {TEST_SINGLE_JOB or 'Disabled'}")
print(f"✓ Load Type: {LOAD_TYPE}")
print(f"✓ Date Range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')} ({(END_DATE - START_DATE).days} days)")
if not HISTORICAL_BACKFILL:
    print(f"✓ Days Back: {DAYS_BACK}")
print(f"✓ Target Table: {TARGET_TABLE}")
print(f"✓ Rows Loaded: {df_spark.count() if df_spark is not None else 0:,}")
print(f"✓ Jobs Processed: {len(job_shortcodes)}")
print(f"✓ Load Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if TEST_SINGLE_JOB:
    print("\n" + "="*80)
    print("⚠ SINGLE JOB TEST COMPLETE")
    print("="*80)
    print(f"✓ Tested historical backfill logic with job: {TEST_SINGLE_JOB}")
    print(f"✓ Verified {df_spark.count() if df_spark is not None else 0:,} activities loaded")
    print("\nIf test looks good, proceed with full backfill:")
    print("  1. Set TEST_SINGLE_JOB = None")
    print("  2. Keep HISTORICAL_BACKFILL = True")
    print("  3. Re-run notebook for full historical load")
elif TEST_MODE:
    print("\n" + "="*80)
    print("⚠ TEST MODE NEXT STEPS:")
    print("="*80)
    if HISTORICAL_BACKFILL:
        print("✓ Tested: Historical backfill (3 months)")
        print("\nTo run FULL backfill from Jan 1, 2024:")
        print("  1. Verify test data looks good in Power BI")
        print("  2. Set TEST_MODE = False")
        print("  3. Keep HISTORICAL_BACKFILL = True")
        print("  4. Re-run notebook for full 1/1/2024 - today backfill")
    else:
        print("✓ Tested: Incremental load (3 days)")
        print("\nTo test historical backfill next:")
        print("  1. Verify test data looks good in Power BI")
        print("  2. Set HISTORICAL_BACKFILL = True")
        print("  3. Keep TEST_MODE = True (tests 3 months)")
        print("  4. Re-run notebook")
        print("\nFor production after testing:")
        print("  1. Set TEST_MODE = False")
        print("  2. Set HISTORICAL_BACKFILL = False")
        print("  3. Schedule for daily runs")
elif HISTORICAL_BACKFILL:
    print("\n✓ FULL HISTORICAL BACKFILL COMPLETE (JAN 2024 - PRESENT)")
    print("\nNext Steps:")
    print("1. Verify data in Power BI")
    print("2. Set HISTORICAL_BACKFILL = False for future runs")
    print("3. Keep TEST_MODE = False")
    print("4. Schedule notebook to run daily")
    print("5. Create relationship: fact_workable_activities[job_shortcode] → dim_workable_jobs[job_shortcode]")
else:
    print("\n✓ INCREMENTAL LOAD COMPLETE")
    print("\nNext Steps:")
    print("1. Verify new/updated records in Power BI")
    print("2. Monitor for any data quality issues")
    print("3. This notebook is ready for daily scheduled execution")

print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
