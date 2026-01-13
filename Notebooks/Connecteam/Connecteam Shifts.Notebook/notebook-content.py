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

# Fabric Notebook: Connecteam Shifts Loader with Explode Strategy (Robust Upserts)
# Purpose: Load shift data from Connecteam API and robustly upsert into Delta tables for Direct Lake
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Last Updated: 2025-11-24 - Added Central Time timezone conversion with DST handling

import requests
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode_outer, explode, col, from_unixtime, from_utc_timestamp, when, lit, 
    coalesce, array_size, hour, current_timestamp, concat_ws, sha2, row_number,
    array_distinct
)
from delta.tables import DeltaTable
from pyspark.sql.window import Window

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

BASE_URL_TEMPLATE = "https://api.connecteam.com/scheduler/v1/schedulers/{scheduler_id}/shifts"

# Dimension table with scheduler metadata
DIM_SCHEDULERS_TABLE = "dim_schedulers"

# =============================================================================
# DATE RANGE CONFIGURATION
# =============================================================================

# Set to True for one-time historical load, False for incremental
HISTORICAL_LOAD = False  # Change to True for initial load, then back to False

if HISTORICAL_LOAD:
    # Historical load: 1/1/25 to 12/18/25
    START_DATE = datetime(2025, 1, 1, 0, 0, 0)
    END_DATE = datetime(2025, 12, 18, 23, 59, 59)
    LOAD_TYPE = "overwrite"
    print("\n⚠ HISTORICAL LOAD MODE - Will overwrite existing tables")
else:
    # Incremental load: 3 days back + 14 days forward
    DAYS_BACK = 3
    DAYS_FORWARD = 14
    NOW = datetime.now()
    START_DATE = (NOW - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0, second=0, microsecond=0)
    END_DATE = (NOW + timedelta(days=DAYS_FORWARD)).replace(hour=23, minute=59, second=59, microsecond=0)
    LOAD_TYPE = "merge"
    print(f"\n✓ INCREMENTAL LOAD MODE - Loading {DAYS_BACK} days back + {DAYS_FORWARD} days forward")

START_TIME_UNIX = int(START_DATE.timestamp())
END_TIME_UNIX = int(END_DATE.timestamp())

# Lakehouse configuration
DATABASE = os.getenv("FABRIC_DB", "")        # e.g., "dbo" or ""
LAKEHOUSE_TABLE_NAME = "connecteam_shifts_exploded" if not DATABASE else f"{DATABASE}.connecteam_shifts_exploded"
LAKEHOUSE_STATUSES_TABLE_NAME = "connecteam_shift_statuses" if not DATABASE else f"{DATABASE}.connecteam_shift_statuses"

# Raw Files path (Fabric Lakehouse)
LAKEHOUSE_RAW_DIR = "Files/raw/connecteam_shifts"

# API params - use updated_at for incremental loads to catch changes
LIMIT = 500
SORT_FIELD = "updated_at" if not HISTORICAL_LOAD else "created_at"
SORT_ORDER = "desc" if not HISTORICAL_LOAD else "asc"

# Keep open shifts (no assigned users) after explode?
KEEP_OPEN_SHIFTS = True

# =============================================================================
# UTILS
# =============================================================================

def print_config():
    print("="*80)
    print(f"{'HISTORICAL' if HISTORICAL_LOAD else 'INCREMENTAL'} LOAD CONFIGURATION")
    print("="*80)
    print(f"Current time: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Loading shifts from: {START_DATE:%Y-%m-%d %H:%M:%S}")
    print(f"                 to: {END_DATE:%Y-%m-%d %H:%M:%S}")
    print(f"Days covered: {(END_DATE - START_DATE).days + 1}")
    print(f"Load strategy: {LOAD_TYPE.upper()}")
    print(f"Sort by: {SORT_FIELD} ({SORT_ORDER})")
    print(f"Target table: {LAKEHOUSE_TABLE_NAME}")
    print(f"Statuses table: {LAKEHOUSE_STATUSES_TABLE_NAME}")
    print(f"Keep open shifts: {KEEP_OPEN_SHIFTS}")
    print(f"Timezone: America/Chicago (Central Time with DST)")
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

# =============================================================================
# STEP 1: EXTRACT
# =============================================================================

def get_connecteam_shifts(scheduler_id, scheduler_name, start_time, end_time, api_token):
    """Fetch shifts for a specific scheduler"""
    url = BASE_URL_TEMPLATE.format(scheduler_id=scheduler_id)
    headers = {"X-API-Key": api_token, "Content-Type": "application/json"}
    all_shifts, offset, page_count = [], 0, 0

    print(f"\n{'='*80}")
    print(f"EXTRACTING SHIFTS: {scheduler_name} (ID: {scheduler_id})")
    print(f"{'='*80}")
    print(f"Date range: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
    print(f"Sorting by: {SORT_FIELD} ({SORT_ORDER})")

    while True:
        page_count += 1
        params = {
            "startTime": start_time,
            "endTime": end_time,
            "sort": SORT_FIELD,
            "order": SORT_ORDER,
            "limit": LIMIT,
            "offset": offset
        }
        try:
            print(f"  Fetching page {page_count} (offset: {offset})...")
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            shifts = data.get("data", {}).get("shifts", [])
            if not shifts:
                print("  No more shifts found. Stopping pagination.")
                break

            all_shifts.extend(shifts)
            print(f"    Retrieved {len(shifts)} shifts (Total: {len(all_shifts)})")

            paging = data.get("paging", {})
            if isinstance(paging, dict) and "offset" in paging:
                next_offset = paging.get("offset")
                offset = offset + LIMIT if next_offset == offset else next_offset
            else:
                offset += LIMIT

            if len(shifts) < LIMIT:
                print(f"  Last page reached (got {len(shifts)} < {LIMIT})")
                break

            time.sleep(0.4)
        except requests.exceptions.RequestException as e:
            print(f"ERROR: API request failed on page {page_count}: {e}")
            raise

    print(f"✓ Completed: {len(all_shifts)} total shifts from {scheduler_name}")
    return all_shifts

# =============================================================================
# STEP 2: TRANSFORM
# =============================================================================

def transform_shifts_exploded(shifts_data, scheduler_id, scheduler_name):
    print("\nStarting transformation...")
    if not shifts_data:
        return None

    pdf = pd.json_normalize(shifts_data)
    print(f"  Initial records: {len(pdf)} | Columns: {len(pdf.columns)}")

    sdf = spark.createDataFrame(pdf)

    # Check if openSpots column exists (not all schedulers have it)
    has_open_spots = "openSpots" in sdf.columns
    
    # CRITICAL: Convert Unix timestamps to UTC first, then to Central Time
    # This handles DST automatically using America/Chicago timezone rules
    base = sdf.select(
        lit(scheduler_id).alias("scheduler_id"),
        lit(scheduler_name).alias("scheduler_name"),
        col("id").alias("shift_id"),
        col("title").alias("shift_title"),
        # Convert to Central Time (handles DST automatically)
        from_utc_timestamp(
            from_unixtime(col("startTime")),
            "America/Chicago"
        ).cast("timestamp").alias("shift_start_datetime"),
        from_utc_timestamp(
            from_unixtime(col("endTime")),
            "America/Chicago"
        ).cast("timestamp").alias("shift_end_datetime"),
        from_utc_timestamp(
            from_unixtime(col("creationTime")),
            "America/Chicago"
        ).cast("timestamp").alias("created_datetime"),
        from_utc_timestamp(
            from_unixtime(col("updateTime")),
            "America/Chicago"
        ).cast("timestamp").alias("updated_datetime"),
        ((col("endTime") - col("startTime")) / 3600).alias("shift_duration_hours"),
        col("jobId").alias("job_id"),
        col("timezone"),
        col("`locationData.gps.address`").alias("location_address"),
        col("isOpenShift").alias("is_open_shift"),
        col("isPublished").alias("is_published"),
        col("isRequireAdminApproval").alias("requires_admin_approval"),
        col("openSpots").cast("double").alias("open_spots") if has_open_spots else lit(None).cast("double").alias("open_spots"),
        col("color"),
        col("assignedUserIds").alias("assignedUserIds"),
        col("statuses").alias("statuses"),
        col("breaks").alias("breaks"),
        coalesce(array_size(col("assignedUserIds")), lit(0)).alias("assigned_user_count"),
        coalesce(array_size(col("statuses")), lit(0)).alias("status_count"),
        coalesce(array_size(col("breaks")), lit(0)).alias("break_count"),
    )
    
    if not has_open_spots:
        print(f"  ⚠ Note: '{scheduler_name}' does not have 'openSpots' field - setting to NULL")

    # De-duplicate assignedUserIds arrays before exploding
    from pyspark.sql.functions import array_distinct
    base = base.withColumn("assignedUserIds", array_distinct(col("assignedUserIds")))

    if KEEP_OPEN_SHIFTS:
        exploded = base.select("*", explode_outer(col("assignedUserIds")).alias("assigned_user_id"))
    else:
        exploded = base.select("*", explode(col("assignedUserIds")).alias("assigned_user_id"))

    final = exploded.select(
        "scheduler_id", "scheduler_name",
        "shift_id", "shift_title",
        "shift_start_datetime", "shift_end_datetime",
        "created_datetime", "updated_datetime",
        "shift_duration_hours", "job_id",
        "timezone", "location_address",
        "is_open_shift", "is_published",
        "requires_admin_approval", "open_spots",
        "color", "assigned_user_count", "status_count", "break_count",
        "assigned_user_id"
    ).withColumn("shift_date", col("shift_start_datetime").cast("date")) \
     .withColumn("shift_start_hour", hour(col("shift_start_datetime"))) \
     .withColumn("etl_loaded_datetime", from_utc_timestamp(current_timestamp(), "America/Chicago")) \
     .withColumn("window_start", from_utc_timestamp(lit(START_DATE).cast("timestamp"), "America/Chicago")) \
     .withColumn("window_end", from_utc_timestamp(lit(END_DATE).cast("timestamp"), "America/Chicago")) \
     .withColumn(
         "is_overnight_shift",
         when(col("shift_start_hour") >= 20, lit(True))
         .when(col("shift_start_hour") <= 6, lit(True))
         .otherwise(lit(False))
     ) \
     .withColumn(
         "shift_type",
         when(col("shift_start_hour").between(6, 13), lit("Day"))
         .when(col("shift_start_hour").between(14, 21), lit("Evening"))
         .otherwise(lit("Night"))
     )

    # Create a proper composite key for this exploded structure
    final = final.withColumn(
        "shift_assignment_key",
        sha2(
            concat_ws(
                "|",
                col("scheduler_id").cast("string"),
                col("shift_id"),
                coalesce(col("assigned_user_id").cast("string"), lit("__OPEN__"))
            ),
            256
        )
    )

    count_rows = final.count()
    print(f"  Transformed to {count_rows} rows (after {'explode_outer' if KEEP_OPEN_SHIFTS else 'explode'})")
    print(f"  Final columns: {len(final.columns)}")
    print(f"  ✓ All timestamps converted to America/Chicago (Central Time with DST)")
    return final

# =============================================================================
# STEP 3: STATUS TABLE
# =============================================================================

def create_statuses_table(shifts_data, scheduler_id, scheduler_name):
    print("\nCreating statuses dimension table...")

    if not shifts_data:
        print("  No statuses found")
        return None

    # Flatten raw JSON to a DataFrame
    pdf = pd.json_normalize(shifts_data)
    sdf = spark.createDataFrame(pdf)

    # Check if statuses column exists and has any non-null values
    if "statuses" not in sdf.columns:
        print("  No statuses column found")
        return None
    
    # Filter to only shifts that actually have statuses before exploding
    sdf_with_statuses = sdf.filter(
        (col("statuses").isNotNull()) & (array_size(col("statuses")) > 0)
    )
    
    # Check if we have any shifts with actual statuses
    if sdf_with_statuses.rdd.isEmpty():
        print("  No shifts with status data found")
        return None

    # Now safe to explode - we know statuses exists and has content
    statuses = (
        sdf_with_statuses
        .select(
            lit(scheduler_id).alias("scheduler_id"),
            lit(scheduler_name).alias("scheduler_name"),
            col("id").alias("shift_id"),
            explode("statuses").alias("st")
        )
        .select(
            "scheduler_id",
            "scheduler_name",
            "shift_id",
            col("st.statusId").alias("status_id"),
            col("st.status").alias("status"),
            col("st.creationTime").alias("creation_time"),
            col("st.updateTime").alias("update_time"),
            col("st.creatingUserId").alias("creating_user_id"),
            col("st.modifyingUserId").alias("modifying_user_id"),
            col("st.assignedUserId").alias("assigned_user_id")
        )
    )

    # Convert epoch seconds → Central Time timestamps (with DST handling)
    statuses = (
        statuses
        .withColumn("creation_datetime", 
            from_utc_timestamp(
                from_unixtime(col("creation_time")),
                "America/Chicago"
            ).cast("timestamp"))
        .withColumn("update_datetime",
            from_utc_timestamp(
                from_unixtime(col("update_time")),
                "America/Chicago"
            ).cast("timestamp"))
        .withColumn("window_start", 
            from_utc_timestamp(lit(START_DATE).cast("timestamp"), "America/Chicago"))
        .withColumn("window_end", 
            from_utc_timestamp(lit(END_DATE).cast("timestamp"), "America/Chicago"))
    )

    # Add shift_assignment_key using the SAME pattern as in transform_shifts_exploded
    statuses = statuses.withColumn(
        "shift_assignment_key",
        sha2(
            concat_ws(
                "|",
                coalesce(col("scheduler_id").cast("string"), lit("")),
                coalesce(col("shift_id").cast("string"), lit("")),
                coalesce(col("assigned_user_id").cast("string"), lit("__OPEN__"))
            ),
            256
        )
    )

    # Add a deterministic status_key (unique per shift + status + assigned_user_id)
    statuses = statuses.withColumn(
        "status_key",
        sha2(
            concat_ws(
                "|",
                coalesce(col("scheduler_id").cast("string"), lit("")),
                coalesce(col("shift_id").cast("string"), lit("")),
                coalesce(col("status_id").cast("string"), lit("")),
                coalesce(col("assigned_user_id").cast("string"), lit("__NULL__"))
            ),
            256
        )
    )

    # Add sequence & "latest" flags per (scheduler_id, shift_id, assigned_user_id)
    w_seq = (
        Window.partitionBy("scheduler_id", "shift_id", "assigned_user_id")
        .orderBy(col("creation_datetime").asc())
    )

    w_latest = (
        Window.partitionBy("scheduler_id", "shift_id", "assigned_user_id")
        .orderBy(col("creation_datetime").desc())
    )

    statuses = statuses.withColumn("status_sequence", row_number().over(w_seq))

    statuses = statuses.withColumn(
        "is_latest_for_shift_user",
        (row_number().over(w_latest) == lit(1))
    )

    count_records = statuses.count()
    print(f"  Created {count_records} status records")
    print(f"  ✓ All status timestamps converted to America/Chicago (Central Time with DST)")
    return statuses


# =============================================================================
# STEP 4: LOAD
# =============================================================================

def table_exists(full_table_name: str) -> bool:
    if '.' in full_table_name:
        db, tbl = full_table_name.split('.', 1)
        return spark.catalog.tableExists(db, tbl)
    return spark.catalog.tableExists(full_table_name)

def migrate_table_schema_if_needed(table_name):
    """Check if table has required columns, offer to recreate if not"""
    if not table_exists(table_name):
        return  # New table, no migration needed
    
    existing_columns = [field.name for field in spark.table(table_name).schema.fields]
    
    missing_columns = []
    if "scheduler_id" not in existing_columns:
        missing_columns.append("scheduler_id")
    if "scheduler_name" not in existing_columns:
        missing_columns.append("scheduler_name")
    
    if missing_columns:
        print(f"\n{'='*80}")
        print(f"SCHEMA MISMATCH DETECTED: {table_name}")
        print(f"{'='*80}")
        print(f"Missing columns: {', '.join(missing_columns)}")
        print("\nThis table was created before multi-scheduler support was added.")
        print("\nRECOMMENDED ACTION:")
        print("1. Set HISTORICAL_LOAD = True at the top of this script")
        print("2. Run the script ONCE to recreate the table with correct schema")
        print("3. Change HISTORICAL_LOAD back to False for future incremental loads")
        print("\nOr manually drop the table and let it recreate:")
        print(f"   spark.sql('DROP TABLE {table_name}')")
        print(f"{'='*80}\n")
        
        raise ValueError(
            f"Schema migration required for {table_name}. "
            f"Please set HISTORICAL_LOAD=True for one run, or drop the table manually."
        )

def load_to_lakehouse(df, table_name, load_type="overwrite", key_expr=None, delete_condition=None):
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
# MAIN
# =============================================================================

def get_active_schedulers():
    """Read scheduler IDs from dimension table"""
    print("\n" + "="*80)
    print("READING ACTIVE SCHEDULERS FROM DIMENSION TABLE")
    print("="*80)
    
    try:
        schedulers_df = spark.table(DIM_SCHEDULERS_TABLE)
        active_schedulers = schedulers_df.filter(col("include_in_shift_loads") == True).collect()
        
        print(f"✓ Found {len(active_schedulers)} active schedulers to process:")
        for row in active_schedulers:
            print(f"  • {row['scheduler_id']:>8} - {row['scheduler_name']}")
        
        return active_schedulers
    except Exception as e:
        print(f"ERROR: Could not read {DIM_SCHEDULERS_TABLE}: {e}")
        print("\nPlease run the dim_schedulers_loader.py notebook first!")
        raise

def main():
    print("="*80)
    print("CONNECTEAM SHIFTS ETL - MULTI-SCHEDULER LOADER")
    print("="*80)
    print(f"Execution started: {datetime.now()}\n")
    print_config()

    # If using overwrite mode, drop both tables to prevent schema conflicts
    if LOAD_TYPE == "overwrite":
        if table_exists(LAKEHOUSE_TABLE_NAME) or table_exists(LAKEHOUSE_STATUSES_TABLE_NAME):
            print("\n" + "="*80)
            print("⚠ OVERWRITE MODE: Dropping tables to prevent schema conflicts")
            print("="*80)
            if table_exists(LAKEHOUSE_TABLE_NAME):
                spark.sql(f"DROP TABLE IF EXISTS {LAKEHOUSE_TABLE_NAME}")
                print(f"✓ Dropped {LAKEHOUSE_TABLE_NAME}")
            if table_exists(LAKEHOUSE_STATUSES_TABLE_NAME):
                spark.sql(f"DROP TABLE IF EXISTS {LAKEHOUSE_STATUSES_TABLE_NAME}")
                print(f"✓ Dropped {LAKEHOUSE_STATUSES_TABLE_NAME}")
            print("="*80 + "\n")

    # Check if schema migration is needed (only if doing merge)
    if LOAD_TYPE == "merge":
        migrate_table_schema_if_needed(LAKEHOUSE_TABLE_NAME)
        migrate_table_schema_if_needed(LAKEHOUSE_STATUSES_TABLE_NAME)
    elif table_exists(LAKEHOUSE_TABLE_NAME):
        existing_columns = [field.name for field in spark.table(LAKEHOUSE_TABLE_NAME).schema.fields]
        if "scheduler_id" not in existing_columns:
            print("\n" + "="*80)
            print("⚠ SCHEMA UPGRADE IN PROGRESS")
            print("="*80)
            print("HISTORICAL_LOAD is set to True and existing table lacks scheduler columns.")
            print("This run will recreate the table with the correct schema.")
            print("After this completes, change HISTORICAL_LOAD back to False for incremental loads.")
            print("="*80 + "\n")

    # Get list of schedulers to process
    active_schedulers = get_active_schedulers()
    
    if not active_schedulers:
        print("WARNING: No active schedulers found. Exiting.")
        return
    
    # Track overall progress
    total_shifts_processed = 0
    total_assignments_loaded = 0
    scheduler_results = []
    
    # Loop through each scheduler
    for idx, scheduler_row in enumerate(active_schedulers, 1):
        scheduler_id = scheduler_row['scheduler_id']
        scheduler_name = scheduler_row['scheduler_name']
        
        print(f"\n{'#'*80}")
        print(f"PROCESSING SCHEDULER {idx}/{len(active_schedulers)}")
        print(f"{'#'*80}")
        
        try:
            # STEP 1: Extract shifts for this scheduler
            shifts_data = get_connecteam_shifts(
                scheduler_id, 
                scheduler_name,
                START_TIME_UNIX, 
                END_TIME_UNIX, 
                API_TOKEN
            )
            
            if not shifts_data:
                print(f"⚠ No shifts data for {scheduler_name}. Skipping.")
                scheduler_results.append({
                    'scheduler_id': scheduler_id,
                    'scheduler_name': scheduler_name,
                    'shifts': 0,
                    'status': 'NO_DATA'
                })
                continue

            # STEP 1b: Write raw JSON
            raw_file_path = f"{LAKEHOUSE_RAW_DIR}/{START_DATE:%Y-%m-%d}_{scheduler_id}_{scheduler_name.replace(' ', '_')}_shifts_raw.json"
            print(f"\nSaving raw JSON to: {raw_file_path}")
            write_raw_json_to_lakehouse(raw_file_path, json.dumps(shifts_data, indent=2))

            # STEP 2: Transform
            df_shifts_exploded = transform_shifts_exploded(shifts_data, scheduler_id, scheduler_name)

            # STEP 3: Load main table
            key_expr_shifts = (
                "(coalesce(t.scheduler_id,'') = coalesce(s.scheduler_id,'')) AND "
                "(coalesce(t.shift_id,'') = coalesce(s.shift_id,'')) AND "
                "(coalesce(cast(t.assigned_user_id as string),'__NULL__') = "
                " coalesce(cast(s.assigned_user_id as string),'__NULL__'))"
            )
            
            delete_condition_shifts = (
                f"t.scheduler_id = {scheduler_id} AND "
                f"t.shift_start_datetime >= CAST('{START_DATE}' AS TIMESTAMP) AND "
                f"t.shift_start_datetime <= CAST('{END_DATE}' AS TIMESTAMP)"
            )

            # Use append mode for shifts when in overwrite mode
            # (we dropped the table at the start)
            shifts_load_type = "append" if LOAD_TYPE == "overwrite" else LOAD_TYPE

            load_to_lakehouse(
                df_shifts_exploded,
                LAKEHOUSE_TABLE_NAME,
                load_type=shifts_load_type,
                key_expr=key_expr_shifts,
                delete_condition=delete_condition_shifts
            )

            # STEP 4: Optional statuses table
            df_statuses = create_statuses_table(shifts_data, scheduler_id, scheduler_name)
            if df_statuses is not None:
                key_expr_statuses = (
                    "(coalesce(t.shift_assignment_key,'') = coalesce(s.shift_assignment_key,'')) AND "
                    "(coalesce(cast(t.status_id as string),'') = coalesce(cast(s.status_id as string),''))"
                )
                delete_condition_statuses = (
                    f"t.scheduler_id = {scheduler_id} AND "
                    f"t.update_datetime >= CAST('{START_DATE}' AS TIMESTAMP) AND "
                    f"t.update_datetime <= CAST('{END_DATE}' AS TIMESTAMP)"
                )
                
                # Use append mode for statuses when shifts are in overwrite mode
                # (we dropped the statuses table at the start)
                statuses_load_type = "append" if LOAD_TYPE == "overwrite" else LOAD_TYPE
                
                load_to_lakehouse(
                    df_statuses,
                    LAKEHOUSE_STATUSES_TABLE_NAME,
                    load_type=statuses_load_type,
                    key_expr=key_expr_statuses,
                    delete_condition=delete_condition_statuses
                )
            
            # Track results
            row_count = df_shifts_exploded.count()
            total_shifts_processed += len(shifts_data)
            total_assignments_loaded += row_count
            
            scheduler_results.append({
                'scheduler_id': scheduler_id,
                'scheduler_name': scheduler_name,
                'shifts': len(shifts_data),
                'assignments': row_count,
                'status': 'SUCCESS'
            })
            
            print(f"\n✓ Completed {scheduler_name}: {len(shifts_data)} shifts, {row_count} assignments")
            
        except Exception as e:
            print(f"\n✗ ERROR processing {scheduler_name}: {e}")
            scheduler_results.append({
                'scheduler_id': scheduler_id,
                'scheduler_name': scheduler_name,
                'shifts': 0,
                'status': f'ERROR: {str(e)[:50]}'
            })
            # Continue with next scheduler rather than failing entire job
            continue

    # Final summary
    print("\n" + "="*80)
    print("ETL COMPLETED")
    print("="*80)
    print(f"Completion time: {datetime.now()}")
    print(f"\nSchedulers processed: {len(active_schedulers)}")
    print(f"Total shifts extracted: {total_shifts_processed:,}")
    print(f"Total assignments loaded: {total_assignments_loaded:,}")
    
    print("\n" + "="*80)
    print("DETAILED RESULTS BY SCHEDULER")
    print("="*80)
    for result in scheduler_results:
        status_icon = "✓" if result['status'] == 'SUCCESS' else "✗"
        if result['status'] == 'SUCCESS':
            print(f"{status_icon} {result['scheduler_name']:30} | Shifts: {result['shifts']:>4} | Assignments: {result['assignments']:>5}")
        else:
            print(f"{status_icon} {result['scheduler_name']:30} | {result['status']}")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
