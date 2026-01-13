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

# Fabric Notebook: NICE CXone Completed Contacts Loader
# Purpose: Extract completed contacts data from NICE CXone API and load to Delta tables
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Last Updated: 2025-12-16 - Patched: token refresh, boundary handling, Retry-After, empty-batch skip, clearer metrics

import requests
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta, timezone
from notebookutils import mssparkutils

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, from_utc_timestamp, lit,
    explode_outer, sha2, concat_ws
)
from delta.tables import DeltaTable

# =============================================================================
# CONFIGURATION
# =============================================================================

# Azure Key Vault Configuration
KEY_VAULT_URL = "https://itt-dataanalytics.vault.azure.net/"
ACCESS_KEY_ID_SECRET_NAME = "NiceInContactKeyIDMBrents"
ACCESS_KEY_SECRET_SECRET_NAME = "NiceInContactSecretKeyMBrents"

# NICE CXone Configuration
FORCE_REGION = "na1"
API_VERSION = "v27.0"
TIMEOUT = 120
MAX_RETRIES = 5
RETRY_DELAY = 5  # base seconds (exponential backoff)

# Pagination settings
TOP = 10000
SKIP_INCREMENT = 10000

# Chunking / batching settings
CHUNK_DAYS = 7
BATCH_CHUNKS = 10  # yield after this many chunks

# Date range configuration
HISTORICAL_LOAD = False  # set False after initial load
NOW = datetime.now(timezone.utc) - timedelta(minutes=15)

if HISTORICAL_LOAD:
    START_DATE = datetime(2025, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
    END_DATE = NOW
    LOAD_TYPE = "overwrite"
    print("\n⚠ HISTORICAL LOAD MODE - Will overwrite existing tables")
else:
    START_DATE = (NOW - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    END_DATE = NOW
    LOAD_TYPE = "merge"
    print("\n✓ INCREMENTAL LOAD MODE - Loading from prior day to 15 minutes ago")

# Lakehouse configuration
DATABASE = os.getenv("FABRIC_DB", "")
LAKEHOUSE_TABLE_NAME = "fact_nicecxone_completed_contacts" if not DATABASE else f"{DATABASE}.fact_nicecxone_completed_contacts"
LAKEHOUSE_TAGS_TABLE_NAME = "fact_nicecxone_contact_tags" if not DATABASE else f"{DATABASE}.fact_nicecxone_contact_tags"

# Raw files path (optional)
LAKEHOUSE_RAW_DIR = "Files/raw/nicecxone_contacts"

# Token refresh configuration
TOKEN_REFRESH_SAFETY_MINUTES = 5

# =============================================================================
# SPARK
# =============================================================================
spark = SparkSession.builder.getOrCreate()

# =============================================================================
# UTILS
# =============================================================================

def print_config():
    print("="*80)
    print(f"{'HISTORICAL' if HISTORICAL_LOAD else 'INCREMENTAL'} LOAD CONFIGURATION")
    print("="*80)
    print(f"Current time: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
    print(f"Loading contacts from: {START_DATE:%Y-%m-%d %H:%M:%S} UTC")
    print(f"                   to: {END_DATE:%Y-%m-%d %H:%M:%S} UTC")
    print(f"Days covered: {(END_DATE - START_DATE).days + 1}")
    print(f"Load strategy: {LOAD_TYPE.upper()}")
    print(f"Target table: {LAKEHOUSE_TABLE_NAME}")
    print(f"Tags table: {LAKEHOUSE_TAGS_TABLE_NAME}")
    print(f"API Version: {API_VERSION}")
    print(f"Region: {FORCE_REGION}")
    print(f"Max records per page: {TOP}")
    print(f"Request timeout: {TIMEOUT}s")
    print(f"Max retries: {MAX_RETRIES}")
    print(f"Chunk size (days): {CHUNK_DAYS}")
    print(f"Batch chunks: {BATCH_CHUNKS}")
    print("="*80)

def ensure_automerge():
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def table_exists(full_table_name: str) -> bool:
    if "." in full_table_name:
        db, tbl = full_table_name.split(".", 1)
        return spark.catalog.tableExists(db, tbl)
    return spark.catalog.tableExists(full_table_name)

def write_raw_json_to_lakehouse(path, payload_str):
    """Write raw JSON to Fabric Lakehouse Files (best-effort)."""
    try:
        try:
            mssparkutils.fs.mkdirs(os.path.dirname(path))
        except Exception:
            pass
        mssparkutils.fs.put(path, payload_str, overwrite=True)
        print(f"  ✓ Raw JSON written to Lakehouse: {path}")
    except Exception as e:
        print(f"  ⚠ Failed to write raw JSON to Lakehouse ({path}): {e}")

def iso_utc_ms(dt: datetime) -> str:
    """ISO 8601 UTC string with millisecond precision, e.g. 2025-12-16T12:34:56.789Z"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

# =============================================================================
# STEP 1: AUTH + DISCOVERY (WITH TOKEN TRACKING)
# =============================================================================

def regional_auth_url(region: str) -> str:
    return f"https://{region}.nice-incontact.com/authentication/v1/token/access-key"

def regional_discovery_url(region: str) -> str:
    return f"https://{region}.nice-incontact.com/.well-known/cxone-configuration"

def get_credentials():
    print("Retrieving NICE CXone credentials from Azure Key Vault...")
    key_id = mssparkutils.credentials.getSecret(KEY_VAULT_URL, ACCESS_KEY_ID_SECRET_NAME)
    key_secret = mssparkutils.credentials.getSecret(KEY_VAULT_URL, ACCESS_KEY_SECRET_SECRET_NAME)
    print("✓ Successfully retrieved credentials from Key Vault\n")
    return key_id, key_secret

def authenticate(access_key_id: str, access_key_secret: str):
    """
    Returns (token, expires_in_seconds, token_expires_at_utc)
    """
    print("\n" + "="*80)
    print("AUTHENTICATING WITH NICE CXONE")
    print("="*80)

    auth_url = regional_auth_url(FORCE_REGION)
    print(f"Auth URL: {auth_url}")

    resp = requests.post(
        auth_url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"accessKeyId": access_key_id, "accessKeySecret": access_key_secret},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    auth_data = resp.json()

    token = auth_data.get("access_token")
    expires_in = auth_data.get("expires_in")

    if not token:
        raise RuntimeError("No access_token in auth response.")

    token_expires_at = None
    if isinstance(expires_in, (int, float)):
        token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))

    print("✓ Authentication SUCCESSFUL!")
    print(f"  Region: {FORCE_REGION}")
    print(f"  Token expires in: {expires_in} seconds" + (f" (≈ {token_expires_at.isoformat()})" if token_expires_at else ""))

    return token, expires_in, token_expires_at

def discover_api_endpoint(token: str) -> str:
    print("\n" + "="*80)
    print("DISCOVERING API ENDPOINT")
    print("="*80)

    disc_url = regional_discovery_url(FORCE_REGION)
    print(f"Discovery URL: {disc_url}")

    try:
        disc = requests.get(
            disc_url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=TIMEOUT,
        )
        disc.raise_for_status()
        disc_json = disc.json()

        api_endpoint = (disc_json.get("api_endpoint") or "").rstrip("/")
        if not api_endpoint:
            api_endpoint = f"https://api-{FORCE_REGION}.niceincontact.com"
            print(f"  ⚠ No api_endpoint in discovery; using synthesized: {api_endpoint}")

        api_base = f"{api_endpoint}/incontactapi"
        print(f"✓ API Base: {api_base}")
        return api_base

    except Exception as e:
        print(f"⚠ Discovery request error: {e}")
        api_endpoint = f"https://api-{FORCE_REGION}.niceincontact.com"
        api_base = f"{api_endpoint}/incontactapi"
        print(f"  ➜ Using synthesized API base: {api_base}")
        return api_base

# Global token state (set in main)
ACCESS_KEY_ID = None
ACCESS_KEY_SECRET = None
TOKEN = None
TOKEN_EXPIRES_AT = None

def ensure_valid_token():
    """
    Proactively refresh token if within TOKEN_REFRESH_SAFETY_MINUTES of expiring.
    """
    global TOKEN, TOKEN_EXPIRES_AT, ACCESS_KEY_ID, ACCESS_KEY_SECRET
    if TOKEN_EXPIRES_AT is None:
        return TOKEN

    refresh_at = TOKEN_EXPIRES_AT - timedelta(minutes=TOKEN_REFRESH_SAFETY_MINUTES)
    if datetime.now(timezone.utc) >= refresh_at:
        print("\n⚠ Token nearing expiration; refreshing...")
        TOKEN, _, TOKEN_EXPIRES_AT = authenticate(ACCESS_KEY_ID, ACCESS_KEY_SECRET)
        print("✓ Token refreshed\n")

    return TOKEN

# =============================================================================
# HTTP helper with Retry-After + 401/403 reauth once
# =============================================================================

def http_get_with_retry(url, headers, params, timeout, allow_reauth=True):
    """
    Returns response.json()
    Retries on timeouts, 408, 429, 503 (honor Retry-After), and reauth once on 401/403.
    """
    global TOKEN, TOKEN_EXPIRES_AT

    retry_count = 0
    did_reauth = False

    while True:
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)

            # Reauth once on auth failures
            if resp.status_code in (401, 403) and allow_reauth and not did_reauth:
                print(f"    ⚠ Received {resp.status_code}; re-authenticating once and retrying...")
                TOKEN, _, TOKEN_EXPIRES_AT = authenticate(ACCESS_KEY_ID, ACCESS_KEY_SECRET)
                headers["Authorization"] = f"Bearer {TOKEN}"
                did_reauth = True
                continue

            # Handle rate limiting / transient service errors
            if resp.status_code in (429, 503):
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_s = int(retry_after)
                    except Exception:
                        wait_s = RETRY_DELAY * (2 ** retry_count)
                else:
                    wait_s = RETRY_DELAY * (2 ** retry_count)

                retry_count += 1
                if retry_count > MAX_RETRIES:
                    resp.raise_for_status()

                print(f"    ⚠ {resp.status_code} received. Waiting {wait_s}s then retrying ({retry_count}/{MAX_RETRIES})...")
                time.sleep(wait_s)
                continue

            # Raise for other non-200 codes
            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.Timeout as e:
            retry_count += 1
            if retry_count > MAX_RETRIES:
                print(f"    ✗ Timeout max retries exceeded. Last error: {e}")
                raise
            delay = RETRY_DELAY * (2 ** (retry_count - 1))
            print(f"    ⚠ Timeout. Retrying in {delay}s ({retry_count}/{MAX_RETRIES})...")
            time.sleep(delay)

        except requests.exceptions.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 408:
                retry_count += 1
                if retry_count > MAX_RETRIES:
                    print(f"    ✗ 408 max retries exceeded. Last error: {e}")
                    raise
                delay = RETRY_DELAY * (2 ** (retry_count - 1))
                print(f"    ⚠ 408 Request Timeout. Retrying in {delay}s ({retry_count}/{MAX_RETRIES})...")
                time.sleep(delay)
            else:
                # Not retryable here
                print(f"    ✗ Non-retryable request error: {e}")
                if getattr(e, "response", None) is not None:
                    print(f"      Response status: {e.response.status_code}")
                    print(f"      Response text: {e.response.text[:500]}")
                raise

# =============================================================================
# STEP 2: EXTRACT
# =============================================================================

def fetch_contacts_single_range(api_base: str, token: str, start_date: datetime, end_date_inclusive: datetime):
    """
    Fetch contacts for a single date range with pagination.
    NOTE: end_date_inclusive is already adjusted (e.g., chunk_end - 1ms) upstream.
    """
    print(f"  Fetching date range: {start_date:%Y-%m-%d %H:%M:%S.%f} to {end_date_inclusive:%Y-%m-%d %H:%M:%S.%f} UTC")

    url = f"{api_base}/services/{API_VERSION}/contacts/completed"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    all_contacts = []
    skip = 0
    page_count = 0

    start_iso = iso_utc_ms(start_date)
    end_iso = iso_utc_ms(end_date_inclusive)

    while True:
        page_count += 1

        # Proactive refresh
        token = ensure_valid_token()
        headers["Authorization"] = f"Bearer {token}"

        params = {
            "startDate": start_iso,
            "endDate": end_iso,
            "top": TOP,
            "skip": skip,
            "orderBy": "contactStartDate asc",
        }

        print(f"  Fetching page {page_count} (skip: {skip})...")

        data = http_get_with_retry(url, headers, params, TIMEOUT, allow_reauth=True)

        contacts = data.get("completedContacts", []) or []
        total_records = data.get("totalRecords", 0) or 0

        if not contacts:
            print("  No more contacts found. Stopping pagination.")
            return all_contacts

        all_contacts.extend(contacts)
        print(f"    ✓ Retrieved {len(contacts)} contacts (Total so far: {len(all_contacts):,} of {total_records:,})")

        # stop conditions
        if len(contacts) < TOP or (total_records and len(all_contacts) >= total_records):
            print("  Last page reached")
            return all_contacts

        # next page
        skip += SKIP_INCREMENT
        time.sleep(1)  # mild rate-limit

def get_completed_contacts_batched(api_base: str, token: str, start_date: datetime, end_date: datetime):
    """
    Chunk overall range into CHUNK_DAYS windows.
    Boundary strategy: treat chunk windows as [start, end) logically by calling the API with endDate = end - 1ms,
    then advance next_start = chunk_end. This avoids overlap while minimizing any chance of gaps.
    """
    print("\n" + "="*80)
    print("EXTRACTING COMPLETED CONTACTS (CHUNKED + BATCHED)")
    print("="*80)
    print(f"Overall date range: {start_date:%Y-%m-%d %H:%M:%S} to {end_date:%Y-%m-%d %H:%M:%S} UTC")

    current_start = start_date
    chunk_num = 0
    batch_num = 0
    chunks_in_batch = 0
    batch_contacts = []

    while current_start < end_date:
        chunk_num += 1
        chunks_in_batch += 1

        chunk_end = min(current_start + timedelta(days=CHUNK_DAYS), end_date)

        # endDate must be inclusive in the API; emulate exclusive end by subtracting 1 millisecond
        chunk_end_inclusive = chunk_end - timedelta(milliseconds=1)
        if chunk_end_inclusive < current_start:
            chunk_end_inclusive = current_start  # safety for very small ranges

        print(f"\n  --- CHUNK {chunk_num}: {current_start:%Y-%m-%d} to {chunk_end:%Y-%m-%d} (exclusive) ---")

        chunk_contacts = fetch_contacts_single_range(api_base, token, current_start, chunk_end_inclusive)
        batch_contacts.extend(chunk_contacts)

        print(f"  Chunk {chunk_num} complete: {len(chunk_contacts):,} contacts (Batch total: {len(batch_contacts):,})")

        # save raw JSON (optional)
        chunk_file = f"{LAKEHOUSE_RAW_DIR}/chunk_{chunk_num:03d}_{current_start:%Y%m%d}_to_{chunk_end:%Y%m%d}.json"
        try:
            write_raw_json_to_lakehouse(chunk_file, json.dumps(chunk_contacts, indent=2))
        except Exception as e:
            print(f"  ⚠ Failed to save chunk JSON: {e}")

        # yield batch?
        if chunks_in_batch >= BATCH_CHUNKS or chunk_end >= end_date:
            batch_num += 1
            print(f"\n  ✓✓✓ BATCH {batch_num} READY: {len(batch_contacts):,} contacts from {chunks_in_batch} chunks ✓✓✓")
            yield batch_contacts
            batch_contacts = []
            chunks_in_batch = 0

        # advance to next chunk start (exclusive end => next start is chunk_end)
        current_start = chunk_end

        if current_start < end_date:
            time.sleep(2)

# =============================================================================
# STEP 3: TRANSFORM
# =============================================================================

def transform_contacts(contacts_data):
    """Transform contacts + extract tags table."""
    print("\n" + "="*80)
    print("TRANSFORMING CONTACTS DATA")
    print("="*80)

    if not contacts_data:
        print("  No contacts to transform")
        return None, None

    pdf = pd.json_normalize(contacts_data)
    print(f"  Initial records: {len(pdf)} | Columns: {len(pdf.columns)}")

    # Ensure nullable columns exist w/ stable dtypes
    expected_cols = {
        "mediaSubTypeId": pd.Int64Dtype(),
        "mediaSubTypeName": "object",
    }
    for c, dtype in expected_cols.items():
        if c not in pdf.columns:
            pdf[c] = pd.Series(dtype=dtype)
            print(f"  Added missing nullable column: {c} ({dtype})")
        elif c == "mediaSubTypeId" and pdf[c].dtype == "object":
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce").astype(pd.Int64Dtype())
            print(f"  Converted {c} from object to Int64")

    sdf = spark.createDataFrame(pdf)

    # Fix all-null columns that can become VOID
    void_type_fixes = {"mediaSubTypeId": "long", "mediaSubTypeName": "string"}
    for col_name, target_type in void_type_fixes.items():
        if col_name in sdf.columns:
            sdf = sdf.withColumn(col_name, col(col_name).cast(target_type))

    # Tags extraction
    tags_df = None
    if "tags" in sdf.columns:
        from pyspark.sql.functions import size
        tags_df = (
            sdf.filter(col("tags").isNotNull() & (size("tags") > 0))
              .select(col("contactId").alias("contact_id"), explode_outer("tags").alias("tag"))
              .select(
                  "contact_id",
                  col("tag.tagId").alias("tag_id"),
                  col("tag.tagName").alias("tag_name"),
              )
              .withColumn("etl_loaded_datetime", from_utc_timestamp(current_timestamp(), "America/Chicago"))
              .withColumn(
                  "contact_tag_key",
                  sha2(concat_ws("|", col("contact_id").cast("string"), col("tag_id").cast("string")), 256),
              )
        )

        if tags_df.rdd.isEmpty():
            tags_df = None
            print("  No tags found in contacts")
        else:
            print("  Tags extracted successfully")

        sdf = sdf.drop("tags")

    # Rename to snake_case and convert selected timestamps
    timestamp_cols = [
        "analyticsProcessedDate", "callbackTime", "contactStartDate",
        "dateACWWarehoused", "dateContactWarehoused", "lastUpdateTime", "refuseTime"
    ]

    rename_mapping = {}
    timestamp_conversions = {}

    for column_name in sdf.columns:
        snake_case = "".join(["_" + c.lower() if c.isupper() else c for c in column_name]).lstrip("_")
        if column_name == snake_case:
            continue
        if column_name in timestamp_cols:
            timestamp_conversions[column_name] = snake_case
        else:
            rename_mapping[column_name] = snake_case

    for old_name, new_name in timestamp_conversions.items():
        sdf = sdf.withColumn(new_name, from_utc_timestamp(col(old_name).cast("timestamp"), "America/Chicago"))

    for old_name, new_name in rename_mapping.items():
        sdf = sdf.withColumnRenamed(old_name, new_name)

    if timestamp_conversions:
        sdf = sdf.drop(*timestamp_conversions.keys())

    # ETL metadata
    sdf = (
        sdf.withColumn("etl_loaded_datetime", from_utc_timestamp(current_timestamp(), "America/Chicago"))
           .withColumn("window_start", from_utc_timestamp(lit(START_DATE).cast("timestamp"), "America/Chicago"))
           .withColumn("window_end", from_utc_timestamp(lit(END_DATE).cast("timestamp"), "America/Chicago"))
    )

    if "contact_start_date" in sdf.columns:
        sdf = sdf.withColumn("contact_date", col("contact_start_date").cast("date"))

    # force clean schema materialization
    sdf = sdf.select(*sdf.columns)

    print("  Transformation complete")
    print(f"  Final columns: {len(sdf.columns)}")
    print("  ✓ All timestamps converted to America/Chicago")

    return sdf, tags_df

# =============================================================================
# STEP 4: LOAD
# =============================================================================

def load_to_lakehouse(df, table_name, load_type="overwrite", key_col="contact_id"):
    print("\n" + "="*80)
    print(f"LOADING TO LAKEHOUSE: {table_name}")
    print("="*80)
    print(f"  Load type: {load_type}")

    ensure_automerge()

    if df is None or df.rdd.isEmpty():
        print("  ⚠ Empty DataFrame, skipping load")
        return

    if load_type == "merge":
        if not table_exists(table_name):
            print("  Table doesn't exist, creating new managed table...")
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            print("  Table created successfully")
        else:
            print("  Performing MERGE (upsert) operation...")
            delta_table = DeltaTable.forName(spark, table_name)
            key_expr = f"t.{key_col} = s.{key_col}"
            (delta_table.alias("t")
                      .merge(df.alias("s"), key_expr)
                      .whenMatchedUpdateAll()
                      .whenNotMatchedInsertAll()
                      .execute())
            print("  Merge completed successfully")

    elif load_type == "overwrite":
        print("  Performing OVERWRITE operation...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print("  Table overwritten successfully")

    print("  ✓ Load completed successfully")

# =============================================================================
# MAIN
# =============================================================================

def main():
    global ACCESS_KEY_ID, ACCESS_KEY_SECRET, TOKEN, TOKEN_EXPIRES_AT

    print("="*80)
    print("NICE CXONE COMPLETED CONTACTS ETL")
    print("="*80)
    print(f"Execution started: {datetime.now(timezone.utc)} UTC\n")
    print_config()

    # overwrite mode: drop tables first
    if LOAD_TYPE == "overwrite":
        if table_exists(LAKEHOUSE_TABLE_NAME) or table_exists(LAKEHOUSE_TAGS_TABLE_NAME):
            print("\n" + "="*80)
            print("⚠ OVERWRITE MODE: Dropping tables")
            print("="*80)
            spark.sql(f"DROP TABLE IF EXISTS {LAKEHOUSE_TABLE_NAME}")
            spark.sql(f"DROP TABLE IF EXISTS {LAKEHOUSE_TAGS_TABLE_NAME}")
            print(f"✓ Dropped {LAKEHOUSE_TABLE_NAME}")
            print(f"✓ Dropped {LAKEHOUSE_TAGS_TABLE_NAME}")
            print("="*80 + "\n")

    ACCESS_KEY_ID, ACCESS_KEY_SECRET = get_credentials()
    TOKEN, _, TOKEN_EXPIRES_AT = authenticate(ACCESS_KEY_ID, ACCESS_KEY_SECRET)
    api_base = discover_api_endpoint(TOKEN)

    batch_num = 0
    total_contacts_processed = 0

    for batch_contacts in get_completed_contacts_batched(api_base, TOKEN, START_DATE, END_DATE):
        batch_num += 1

        if not batch_contacts:
            print(f"\n⚠ Batch {batch_num} was empty. Skipping.")
            continue

        # Deduplicate within the batch by contactId (safety)
        seen = set()
        deduped = []
        for c in batch_contacts:
            cid = c.get("contactId")
            if cid is not None and cid not in seen:
                seen.add(cid)
                deduped.append(c)

        removed = len(batch_contacts) - len(deduped)
        if removed:
            print(f"  Removed {removed} duplicate contacts within batch")

        # Skip if empty AFTER dedupe (Claude note)
        if not deduped:
            print(f"\n⚠ Batch {batch_num} was empty after deduplication. Skipping.")
            continue

        batch_count = len(deduped)
        total_contacts_processed += batch_count

        print("\n" + "="*80)
        print(f"PROCESSING BATCH {batch_num}")
        print("="*80)
        print(f"Batch size (post-dedupe): {batch_count:,} contacts")
        print(f"Total processed across batches (post-dedupe): {total_contacts_processed:,} contacts")

        df_contacts, df_tags = transform_contacts(deduped)
        if df_contacts is None:
            print("  ⚠ No contacts to load in this batch. Skipping.")
            continue

        # First batch of historical overwrite -> overwrite, subsequent -> merge
        load_mode = "overwrite" if (batch_num == 1 and LOAD_TYPE == "overwrite") else "merge"

        load_to_lakehouse(df_contacts, LAKEHOUSE_TABLE_NAME, load_type=load_mode, key_col="contact_id")

        if df_tags is not None:
            load_to_lakehouse(df_tags, LAKEHOUSE_TAGS_TABLE_NAME, load_type=load_mode, key_col="contact_tag_key")

        print(f"\n✓ Batch {batch_num} completed successfully")

        # memory cleanup
        del batch_contacts, deduped, df_contacts, df_tags
        import gc
        gc.collect()

    print("\n" + "="*80)
    print("ETL COMPLETED")
    print("="*80)
    print(f"Completion time: {datetime.now(timezone.utc)} UTC")
    print(f"Total batches processed: {batch_num}")
    print(f"Total contacts processed across batches (post-dedupe): {total_contacts_processed:,}")

    final_contacts = None
    if table_exists(LAKEHOUSE_TABLE_NAME):
        final_contacts = spark.table(LAKEHOUSE_TABLE_NAME).count()
        print(f"Unique contacts in table (post-merge): {final_contacts:,}")

    if table_exists(LAKEHOUSE_TAGS_TABLE_NAME):
        final_tags = spark.table(LAKEHOUSE_TAGS_TABLE_NAME).count()
        print(f"Tags rows in table (post-merge): {final_tags:,}")

    print("="*80)

if __name__ == "__main__":
    main()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
