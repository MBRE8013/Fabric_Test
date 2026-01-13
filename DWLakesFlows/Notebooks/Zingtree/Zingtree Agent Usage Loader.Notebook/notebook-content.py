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

# Fabric Notebook: Zingtree Agent Usage Loader
# Purpose: Extract agent usage data from Zingtree API and load to Delta Lake
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Last Updated: 2024-12-18
# API Documentation: https://zingtree.com/api/rest

# %%
# Setup and Configuration

import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, current_timestamp, lit, to_date, to_timestamp,
    count, countDistinct, sum as spark_sum, avg, max as spark_max
)
from datetime import datetime, timedelta
import json
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access

# Initialize Spark session
spark = SparkSession.builder.appName("ZingtreeAgentUsageLoader").getOrCreate()

print("="*80)
print("ZINGTREE AGENT USAGE DATA LOADER")
print("="*80)
print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80 + "\n")

# %%
# Configuration Parameters

# Azure Key Vault Configuration
KEY_VAULT_URL = "https://itt-dataanalytics.vault.azure.net/"
SECRET_NAME = "ZingtreeKeyMBrents"

# Retrieve API token from Key Vault
print("Retrieving Zingtree API key from Azure Key Vault...")
try:
    ZINGTREE_API_KEY = mssparkutils.credentials.getSecret(KEY_VAULT_URL, SECRET_NAME)
    print("✓ Successfully retrieved API key from Key Vault")
except Exception as e:
    print(f"✗ Error retrieving API key from Key Vault: {str(e)}")
    print("  Make sure:")
    print("  1. The Key Vault is linked to your Fabric workspace")
    print("  2. You have 'Get' permissions on the secret")
    print("  3. The secret name is correct: ZingtreeKeyMBrents")
    raise

# Zingtree API Configuration
ZINGTREE_BASE_URL = "https://zingtree.com/api/v1"

# Lakehouse Configuration
AGENT_SESSIONS_TABLE = "fact_zingtree_agent_sessions"
AGENT_USAGE_DAILY_TABLE = "fact_zingtree_agent_usage_daily"

# Load Mode Configuration
LOAD_MODE = "test"  # Options: 'historical', 'incremental', 'test'
HISTORICAL_START_DATE = "2023-01-01"  # Used only for historical load
INCREMENTAL_LOOKBACK_DAYS = 3  # For incremental, go back N days to catch late arrivals
TEST_START_DATE = "2025-11-1"  # For test mode: specific start date
TEST_END_DATE = "2025-11-30"  # For test mode: specific end date

# Data Filters
FILTER_PRODUCTION_ONLY = True  # Set to False to include all session types

print(f"\nConfiguration loaded:")
print(f"  Load Mode: {LOAD_MODE}")
print(f"  Base URL: {ZINGTREE_BASE_URL}")
print(f"  Sessions Table: {AGENT_SESSIONS_TABLE}")
print(f"  Agent Usage Table: {AGENT_USAGE_DAILY_TABLE}")
print(f"  Production Only: {FILTER_PRODUCTION_ONLY}")
if LOAD_MODE == "historical":
    print(f"  Historical Start: {HISTORICAL_START_DATE}")
elif LOAD_MODE == "test":
    print(f"  Test Date Range: {TEST_START_DATE} to {TEST_END_DATE}")
else:
    print(f"  Lookback Days: {INCREMENTAL_LOOKBACK_DAYS}")
print()

# %%
# Helper Functions

def make_zingtree_request(endpoint: str) -> dict:
    """
    Make GET request to Zingtree API with proper error handling
    
    Args:
        endpoint: API endpoint path (e.g., 'tree/get_trees')
        
    Returns:
        JSON response as dictionary
    """
    url = f"{ZINGTREE_BASE_URL}/{endpoint}"
    headers = {
        "X-Api-Key": ZINGTREE_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            print(f"✗ Not Found (404): {endpoint}")
        elif response.status_code == 500:
            print(f"✗ Server Error (500): Operation not supported")
        else:
            print(f"✗ HTTP Error {response.status_code}: {str(e)}")
        raise
    except Exception as e:
        print(f"✗ Request failed: {str(e)}")
        raise


def get_tree_metadata() -> pd.DataFrame:
    """
    Get all trees to build tree_id to tree_name/tags lookup
    Returns DataFrame with tree metadata
    """
    print("Fetching tree metadata...")
    try:
        response = make_zingtree_request("tree/get_trees")
        trees = response.get('trees', [])
        
        if not trees:
            print("⚠ No trees found in organization")
            return pd.DataFrame()
        
        df = pd.DataFrame(trees)
        print(f"✓ Retrieved {len(df)} trees")
        return df
    except Exception as e:
        print(f"✗ Error fetching tree metadata: {str(e)}")
        return pd.DataFrame()


def get_agent_sessions(agent: str = "*", start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Get agent sessions from Zingtree API
    
    Args:
        agent: Agent email or '*' for all agents
        start_date: Start date as 'YYYY-MM-DD'
        end_date: End date as 'YYYY-MM-DD'
        
    Returns:
        DataFrame with session data
    """
    # Default to last 30 days if not specified
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    endpoint = f"agent_sessions/{agent}/{start_date}/{end_date}"
    print(f"\nFetching agent sessions from {start_date} to {end_date}...")
    
    try:
        response = make_zingtree_request(endpoint)
        
        if response['count'] == 0:
            print(f"⚠ No sessions found for date range {start_date} to {end_date}")
            return pd.DataFrame()
        
        sessions = response.get('sessions', [])
        df = pd.DataFrame(sessions)
        
        # Add metadata columns
        df['report_start_date'] = start_date
        df['report_end_date'] = end_date
        df['extract_timestamp'] = datetime.now()
        
        print(f"✓ Retrieved {len(df)} sessions")
        return df
        
    except Exception as e:
        print(f"✗ Error fetching agent sessions: {str(e)}")
        return pd.DataFrame()


def enrich_sessions_with_tree_metadata(sessions_df: pd.DataFrame, trees_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich session data with tree name and tags
    """
    if sessions_df.empty or trees_df.empty:
        return sessions_df
    
    print("\nEnriching sessions with tree metadata...")
    
    # Create lookup dictionary
    tree_lookup = trees_df.set_index('tree_id')[['name', 'tags']].to_dict('index')
    
    # Map tree_id to tree_name and tags
    sessions_df['tree_name'] = sessions_df['tree_id'].map(
        lambda x: tree_lookup.get(str(x), {}).get('name', 'Unknown')
    )
    sessions_df['tree_tags'] = sessions_df['tree_id'].map(
        lambda x: tree_lookup.get(str(x), {}).get('tags', '')
    )
    
    # Determine session_type based on tags
    sessions_df['session_type'] = sessions_df['tree_tags'].apply(
        lambda tags: 'production' if 'production' in str(tags).lower()
        else 'test' if 'test' in str(tags).lower()
        else 'production'  # default to production
    )
    
    print(f"✓ Enriched {len(sessions_df)} sessions with tree metadata")
    return sessions_df


def convert_pst_to_central(sessions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert PST timestamps to Central Time with daylight savings handling
    
    Zingtree API returns times in PST. This converts to Central Time (CST/CDT).
    """
    if sessions_df.empty:
        return sessions_df
    
    print("\nConverting timestamps from PST to Central Time...")
    
    # Convert start_time string to datetime
    sessions_df['start_time_dt'] = pd.to_datetime(sessions_df['start_time'])
    
    # Localize as PST (Pacific/Los_Angeles handles PST/PDT automatically)
    sessions_df['start_time_dt'] = sessions_df['start_time_dt'].dt.tz_localize('America/Los_Angeles')
    
    # Convert to Central Time (America/Chicago handles CST/CDT automatically)
    sessions_df['start_time_central'] = sessions_df['start_time_dt'].dt.tz_convert('America/Chicago')
    
    # Create a clean string version without timezone suffix for display
    sessions_df['start_time'] = sessions_df['start_time_central'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Also convert last_click_time if it exists
    if 'last_click_time' in sessions_df.columns:
        sessions_df['last_click_time_dt'] = pd.to_datetime(sessions_df['last_click_time'])
        sessions_df['last_click_time_dt'] = sessions_df['last_click_time_dt'].dt.tz_localize('America/Los_Angeles')
        sessions_df['last_click_time_central'] = sessions_df['last_click_time_dt'].dt.tz_convert('America/Chicago')
        sessions_df['last_click_time'] = sessions_df['last_click_time_central'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Drop temporary datetime columns
    sessions_df = sessions_df.drop(columns=['start_time_dt', 'start_time_central'], errors='ignore')
    if 'last_click_time_dt' in sessions_df.columns:
        sessions_df = sessions_df.drop(columns=['last_click_time_dt', 'last_click_time_central'], errors='ignore')
    
    print(f"✓ Converted timestamps to Central Time (handles CST/CDT automatically)")
    return sessions_df


def calculate_agent_usage_metrics(sessions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate agent-level usage metrics from session data
    Matches Zingtree Agent Usage report structure
    """
    if sessions_df.empty:
        return pd.DataFrame()
    
    print("\nCalculating agent usage metrics...")
    
    # Convert start_time to date for counting active days
    sessions_df['session_date'] = pd.to_datetime(sessions_df['start_time']).dt.date
    
    # Convert numeric columns
    sessions_df['total_score'] = pd.to_numeric(sessions_df['total_score'], errors='coerce')
    sessions_df['duration'] = pd.to_numeric(sessions_df.get('duration', 0), errors='coerce')
    
    # Group by agent and calculate metrics
    agent_metrics = sessions_df.groupby('agent').agg({
        'session_id': 'count',  # total sessions
        'session_date': 'nunique',  # days active
        'resolution_state': lambda x: (x == 'Y').sum(),  # resolved sessions
        'total_score': 'mean',  # average score
        'duration': 'sum'  # total duration
    }).reset_index()
    
    # Rename columns to match report structure
    agent_metrics.columns = [
        'source_agent',
        'active_sessions',
        'days_active',
        'resolved_sessions',
        'avg_score',
        'total_duration_seconds'
    ]
    
    # Calculate additional metrics
    agent_metrics['unresolved_sessions'] = sessions_df.groupby('agent')['resolution_state'].apply(
        lambda x: (x == 'N').sum()
    ).values
    
    agent_metrics['unknown_resolution'] = sessions_df.groupby('agent')['resolution_state'].apply(
        lambda x: (x == '?').sum()
    ).values
    
    # Handle anonymous agents
    agent_metrics['source_agent'] = agent_metrics['source_agent'].replace('', '(Anonymous)')
    
    # Sort by active sessions descending
    agent_metrics = agent_metrics.sort_values('active_sessions', ascending=False)
    
    # Add summary stats
    total_agents = len(agent_metrics)
    total_sessions = agent_metrics['active_sessions'].sum()
    
    print(f"✓ Calculated metrics for {total_agents} agents")
    print(f"  Total Active Sessions: {total_sessions:,}")
    print(f"  Avg Sessions per Agent: {total_sessions/total_agents:.1f}")
    
    return agent_metrics


# %%
# Main Data Extraction Logic

print("\n" + "="*80)
print("STEP 1: FETCH TREE METADATA")
print("="*80)

trees_df = get_tree_metadata()

# %%
print("\n" + "="*80)
print("STEP 2: DETERMINE DATE RANGE")
print("="*80)

if LOAD_MODE == "historical":
    start_date = HISTORICAL_START_DATE
    end_date = datetime.now().strftime('%Y-%m-%d')
    print(f"Historical Load: {start_date} to {end_date}")
elif LOAD_MODE == "test":
    start_date = TEST_START_DATE
    end_date = TEST_END_DATE
    print(f"Test Load: {start_date} to {end_date} (Last 7 Days)")
else:
    # Incremental: Go back N days to catch any late arrivals
    start_date = (datetime.now() - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    print(f"Incremental Load: {start_date} to {end_date} ({INCREMENTAL_LOOKBACK_DAYS} day lookback)")

# %%
print("\n" + "="*80)
print("STEP 3: EXTRACT AGENT SESSIONS")
print("="*80)

sessions_df = get_agent_sessions(agent='*', start_date=start_date, end_date=end_date)

if sessions_df.empty:
    print("\n⚠ No session data retrieved. Exiting.")
    raise SystemExit(0)

# %%
print("\n" + "="*80)
print("STEP 4: ENRICH WITH TREE METADATA")
print("="*80)

sessions_enriched_df = enrich_sessions_with_tree_metadata(sessions_df, trees_df)

# Convert timestamps from PST to Central Time
sessions_enriched_df = convert_pst_to_central(sessions_enriched_df)

# Filter to only Production sessions if configured
if FILTER_PRODUCTION_ONLY:
    print("\nFiltering to Production sessions only...")
    total_sessions_before = len(sessions_enriched_df)
    sessions_enriched_df = sessions_enriched_df[sessions_enriched_df['session_type'] == 'production']
    total_sessions_after = len(sessions_enriched_df)
    print(f"✓ Filtered from {total_sessions_before:,} to {total_sessions_after:,} sessions")
    print(f"  Excluded {total_sessions_before - total_sessions_after:,} non-production sessions")
else:
    print("\n⚠ Including ALL session types (production, test, and unknown)")
    print(f"  Total sessions: {len(sessions_enriched_df):,}")

# %%
print("\n" + "="*80)
print("STEP 5: CALCULATE AGENT USAGE METRICS")
print("="*80)

agent_metrics_df = calculate_agent_usage_metrics(sessions_enriched_df)

# Display sample of results
print("\nTop 10 Agents by Session Count:")
print(agent_metrics_df[['source_agent', 'days_active', 'active_sessions', 'resolved_sessions']].head(10).to_string(index=False))

# %%
print("\n" + "="*80)
print("STEP 6: CONVERT TO SPARK DATAFRAMES")
print("="*80)

# Convert sessions to Spark DataFrame
print("\nConverting sessions to Spark DataFrame...")
sessions_spark_df = spark.createDataFrame(sessions_enriched_df)

# Add processing metadata
sessions_spark_df = sessions_spark_df \
    .withColumn('load_timestamp', current_timestamp()) \
    .withColumn('load_mode', lit(LOAD_MODE)) \
    .withColumn('data_date', to_date(col('start_time')))

print(f"✓ Created Spark DataFrame with {sessions_spark_df.count():,} session records")

# Convert agent metrics to Spark DataFrame
print("\nConverting agent metrics to Spark DataFrame...")
agent_metrics_spark_df = spark.createDataFrame(agent_metrics_df)

# Add date range metadata
agent_metrics_spark_df = agent_metrics_spark_df \
    .withColumn('report_start_date', lit(start_date)) \
    .withColumn('report_end_date', lit(end_date)) \
    .withColumn('extract_timestamp', current_timestamp()) \
    .withColumn('load_mode', lit(LOAD_MODE))

print(f"✓ Created Spark DataFrame with {agent_metrics_spark_df.count()} agent records")

# %%
print("\n" + "="*80)
print("STEP 7: SAVE TO DELTA LAKE")
print("="*80)

# Save detailed sessions fact table
print(f"\nSaving sessions to: {AGENT_SESSIONS_TABLE}")

try:
    if LOAD_MODE == "historical" or LOAD_MODE == "test":
        # Historical/Test load: overwrite
        sessions_spark_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(AGENT_SESSIONS_TABLE)
        print(f"✓ Overwrote {AGENT_SESSIONS_TABLE} with {sessions_spark_df.count():,} records")
    else:
        # Incremental load: merge/upsert based on session_id
        from delta.tables import DeltaTable
        
        # Check if table exists
        table_exists = spark.catalog.tableExists(AGENT_SESSIONS_TABLE)
        
        if table_exists:
            # Table exists - perform merge
            delta_table = DeltaTable.forName(spark, AGENT_SESSIONS_TABLE)
            
            delta_table.alias("target").merge(
                sessions_spark_df.alias("source"),
                "target.session_id = source.session_id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            print(f"✓ Merged updates into {AGENT_SESSIONS_TABLE}")
        else:
            # Table doesn't exist - create it
            sessions_spark_df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(AGENT_SESSIONS_TABLE)
            print(f"✓ Created {AGENT_SESSIONS_TABLE} with {sessions_spark_df.count():,} records")
            
except Exception as e:
    print(f"✗ Error saving sessions table: {str(e)}")
    raise

# Save agent usage daily summary
print(f"\nSaving agent usage metrics to: {AGENT_USAGE_DAILY_TABLE}")

try:
    if LOAD_MODE == "historical" or LOAD_MODE == "test":
        # Historical/Test load: overwrite
        agent_metrics_spark_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(AGENT_USAGE_DAILY_TABLE)
        print(f"✓ Overwrote {AGENT_USAGE_DAILY_TABLE} with {agent_metrics_spark_df.count()} records")
    else:
        # Incremental load: append (each run is a snapshot for the date range)
        agent_metrics_spark_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(AGENT_USAGE_DAILY_TABLE)
        print(f"✓ Appended {agent_metrics_spark_df.count()} records to {AGENT_USAGE_DAILY_TABLE}")
            
except Exception as e:
    print(f"✗ Error saving agent usage table: {str(e)}")
    raise

# %%
print("\n" + "="*80)
print("STEP 8: DATA QUALITY VALIDATION")
print("="*80)

# Reload and validate
print("\nValidating saved data...")

sessions_validation = spark.table(AGENT_SESSIONS_TABLE)
usage_validation = spark.table(AGENT_USAGE_DAILY_TABLE)

print(f"\nSessions Table ({AGENT_SESSIONS_TABLE}):")
print(f"  Total Records: {sessions_validation.count():,}")
print(f"  Unique Sessions: {sessions_validation.select('session_id').distinct().count():,}")
print(f"  Date Range: {sessions_validation.select('data_date').agg({'data_date': 'min'}).collect()[0][0]} to {sessions_validation.select('data_date').agg({'data_date': 'max'}).collect()[0][0]}")
print(f"  Unique Agents: {sessions_validation.select('agent').distinct().count()}")

print(f"\nAgent Usage Table ({AGENT_USAGE_DAILY_TABLE}):")
print(f"  Total Records: {usage_validation.count():,}")
print(f"  Unique Agents: {usage_validation.select('source_agent').distinct().count()}")

# Show sample records
print("\nSample Session Records:")
sessions_validation.select(
    'session_id', 'agent', 'tree_name', 'start_time', 
    'resolution_state', 'session_type', 'data_date'
).show(5, truncate=False)

print("\nSample Agent Usage Records:")
usage_validation.select(
    'source_agent', 'days_active', 'active_sessions', 
    'resolved_sessions', 'report_start_date', 'report_end_date'
).show(5, truncate=False)

# %%
print("\n" + "="*80)
print("EXECUTION SUMMARY")
print("="*80)
print(f"Load Mode: {LOAD_MODE}")
print(f"Date Range: {start_date} to {end_date}")
print(f"Sessions Processed: {sessions_spark_df.count():,}")
print(f"Agents Analyzed: {agent_metrics_spark_df.count()}")
print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)
print("✓ ZINGTREE AGENT USAGE LOAD COMPLETED SUCCESSFULLY")
print("="*80 + "\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
