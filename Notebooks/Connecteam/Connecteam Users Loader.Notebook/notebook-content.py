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

# Fabric Notebook: Connecteam Users Dimension Loader
# Purpose: Load user/employee data from Connecteam API to create Dim_Users table
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Last Updated: 2025-11-13

import requests
import pandas as pd
from datetime import datetime
import json
import time
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, from_unixtime
)
from delta.tables import DeltaTable

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

# API Configuration
USERS_API_URL = "https://api.connecteam.com/users/v1/users"

# Lakehouse configuration
LAKEHOUSE_TABLE_NAME = "dim_connecteam_users"

# Load strategy: "overwrite" for dimension tables (full refresh each time)
# Users don't change frequently, so full refresh is simpler than tracking changes
LOAD_TYPE = "overwrite"  # Options: "overwrite", "merge"

# API parameters
LIMIT = 500  # Max records per page (if API supports pagination)

print("="*80)
print("CONNECTEAM USERS DIMENSION LOADER")
print("="*80)
print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"API endpoint: {USERS_API_URL}")
print(f"Load strategy: {LOAD_TYPE.upper()}")
print(f"Target table: {LAKEHOUSE_TABLE_NAME}")
print("="*80)

# =============================================================================
# STEP 1: EXTRACT - GET USERS FROM API
# =============================================================================

def get_connecteam_users(api_token):
    """
    Fetch all users from Connecteam API with pagination handling
    
    Args:
        api_token: API token for authentication
    
    Returns:
        List of all user records
    """
    
    headers = {
        "X-API-Key": api_token,
        "Content-Type": "application/json"
    }
    
    all_users = []
    offset = 0
    page_count = 0
    
    print(f"\nStarting API extraction...")
    
    while True:
        page_count += 1
        
        # Build request parameters
        # Note: Connecteam Users API may have different pagination params
        # Adjust based on actual API documentation
        params = {
            "limit": LIMIT,
            "offset": offset
        }
        
        try:
            print(f"  Fetching page {page_count} (offset: {offset})...")
            response = requests.get(USERS_API_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract users from response
            # API response structure may vary - adjust as needed
            if isinstance(data, dict):
                users = data.get("data", {}).get("users", [])
                if not users:
                    users = data.get("users", [])
                if not users:
                    users = [data] if data else []
            elif isinstance(data, list):
                users = data
            else:
                users = []
            
            if not users:
                print(f"  No more users found. Stopping pagination.")
                break
            
            all_users.extend(users)
            print(f"    Retrieved {len(users)} users (Total: {len(all_users)})")
            
            # Check for more pages
            # Stop if we got fewer records than the limit (last page)
            if len(users) < LIMIT:
                print(f"  Last page reached (got {len(users)} < {LIMIT})")
                break
            
            # Update offset for next iteration
            offset += LIMIT
            
            # Rate limiting - be nice to the API
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"ERROR: API request failed on page {page_count}: {e}")
            
            # If we got some users already, return what we have
            if all_users:
                print(f"WARNING: Returning {len(all_users)} users collected so far")
                break
            else:
                raise
    
    print(f"\nCompleted extraction: {len(all_users)} total users across {page_count} pages")
    return all_users

# =============================================================================
# STEP 2: TRANSFORM - CREATE DIMENSION TABLE
# =============================================================================

def parse_custom_fields(users_data):
    """
    Parse the customFields array to extract Title, Department, etc.
    Returns a dict mapping userId to parsed custom fields
    """
    custom_fields_map = {}
    
    for user in users_data:
        user_id = user.get('userId')
        custom_fields = user.get('customFields', [])
        
        parsed = {
            'title': None,
            'department': None,
            'employment_type': None,
            'employment_start_date': None,
            'direct_manager_id': None,
            'employee_id': None,
            'primary_team': None,
            'credentials': None,
            'licensure': None,
            'customer': None,
            'all_teams': None
        }
        
        for field in custom_fields:
            field_name = field.get('name', '').lower()
            field_type = field.get('type')
            field_value = field.get('value')
            
            if field_name == 'title':
                parsed['title'] = field_value
            
            elif field_name == 'department' and isinstance(field_value, list) and len(field_value) > 0:
                parsed['department'] = field_value[0].get('value')
            
            elif field_name == 'employment type' and isinstance(field_value, list) and len(field_value) > 0:
                parsed['employment_type'] = field_value[0].get('value')
            
            elif field_name == 'employment start date':
                parsed['employment_start_date'] = field_value
            
            elif field_name == 'direct manager' and field_type == 'directmanager':
                parsed['direct_manager_id'] = field_value
            
            elif field_name == 'employee':
                parsed['employee_id'] = field_value
            
            elif field_name == 'primary team' and isinstance(field_value, list) and len(field_value) > 0:
                parsed['primary_team'] = field_value[0].get('value')
            
            elif field_name == 'credentials' and isinstance(field_value, list) and len(field_value) > 0:
                parsed['credentials'] = field_value[0].get('value')
            
            elif field_name == 'licensure' and isinstance(field_value, list):
                # Concatenate multiple license states
                licenses = [item.get('value') for item in field_value if item.get('value')]
                parsed['licensure'] = ', '.join(licenses) if licenses else None
            
            elif field_name == 'customer' and isinstance(field_value, list):
                # Concatenate multiple customers
                customers = [item.get('value') for item in field_value if item.get('value')]
                parsed['customer'] = ', '.join(customers) if customers else None
            
            elif field_name == 'all teams' and isinstance(field_value, list):
                # Concatenate all teams
                teams = [item.get('value') for item in field_value if item.get('value')]
                parsed['all_teams'] = ', '.join(teams) if teams else None
        
        custom_fields_map[user_id] = parsed
    
    return custom_fields_map

def transform_users_dimension(users_data):
    """
    Transform users data into dimension table format
    
    Args:
        users_data: List of user dictionaries from API
    
    Returns:
        PySpark DataFrame with dimension table structure
    """
    
    print(f"\nStarting transformation...")
    
    # First, parse custom fields
    print(f"  Parsing custom fields...")
    custom_fields_map = parse_custom_fields(users_data)
    
    # Add parsed custom fields back to user records
    for user in users_data:
        user_id = user.get('userId')
        if user_id in custom_fields_map:
            user.update(custom_fields_map[user_id])
    
    # Convert to pandas - but drop the complex nested columns
    # We need to drop: customFields, smartGroupsIds (these are arrays/nested objects)
    users_data_clean = []
    for user in users_data:
        user_clean = {k: v for k, v in user.items() if k not in ['customFields', 'smartGroupsIds']}
        users_data_clean.append(user_clean)
    
    df_pandas = pd.DataFrame(users_data_clean)
    
    print(f"  Initial records: {len(df_pandas)}")
    print(f"  Columns found: {len(df_pandas.columns)}")
    
    # Create Spark DataFrame (now without problematic nested columns)
    df_spark = spark.createDataFrame(df_pandas)
    
    # Transform into standard dimension table structure
    from pyspark.sql.functions import concat, lit as spark_lit
    
    df_dimension = df_spark.select(
        # Primary key
        col("userId").alias("user_id"),
        
        # Basic info - Combine firstName and lastName
        concat(
            coalesce(col("firstName"), spark_lit("")),
            spark_lit(" "),
            coalesce(col("lastName"), spark_lit(""))
        ).alias("user_name"),
        
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("email").alias("email"),
        col("phoneNumber").alias("phone"),
        
        # Employment info from custom fields (now as regular columns)
        col("title").alias("job_title") if "title" in df_spark.columns else spark_lit(None).alias("job_title"),
        col("department").alias("department") if "department" in df_spark.columns else spark_lit(None).alias("department"),
        col("employment_type").alias("employment_type") if "employment_type" in df_spark.columns else spark_lit(None).alias("employment_type"),
        col("employment_start_date").alias("employment_start_date") if "employment_start_date" in df_spark.columns else spark_lit(None).alias("employment_start_date"),
        col("employee_id").alias("employee_id") if "employee_id" in df_spark.columns else spark_lit(None).alias("employee_id"),
        col("primary_team").alias("primary_team") if "primary_team" in df_spark.columns else spark_lit(None).alias("primary_team"),
        col("all_teams").alias("all_teams") if "all_teams" in df_spark.columns else spark_lit(None).alias("all_teams"),
        
        # Manager relationship
        col("direct_manager_id").cast("long").alias("direct_manager_id") if "direct_manager_id" in df_spark.columns else spark_lit(None).cast("long").alias("direct_manager_id"),
        
        # Professional credentials
        col("credentials").alias("credentials") if "credentials" in df_spark.columns else spark_lit(None).alias("credentials"),
        col("licensure").alias("licensure") if "licensure" in df_spark.columns else spark_lit(None).alias("licensure"),
        col("customer").alias("customer") if "customer" in df_spark.columns else spark_lit(None).alias("customer"),
        
        # System fields
        col("userType").alias("user_type"),
        col("kioskCode").alias("kiosk_code"),
        
        # Status
        col("isArchived").alias("is_archived"),
        
        # Dates - Convert from Unix timestamps (seconds for these fields)
        from_unixtime(col("createdAt")).cast("timestamp").alias("created_at"),
        from_unixtime(col("modifiedAt")).cast("timestamp").alias("modified_at"),
        from_unixtime(col("lastLogin")).cast("timestamp").alias("last_login_at"),
        
        # ETL metadata
        spark_lit(datetime.now()).cast("timestamp").alias("etl_loaded_datetime")
    )
    
    # Add derived columns
    df_dimension = df_dimension.withColumn(
        "full_name",
        coalesce(col("user_name"), lit("Unknown"))
    )
    
    # Create is_active flag (inverse of is_archived)
    df_dimension = df_dimension.withColumn(
        "is_active",
        when(col("is_archived") == False, True)
        .when(col("is_archived") == True, False)
        .otherwise(True)
    )
    
    df_dimension = df_dimension.withColumn(
        "status",
        when(col("is_archived") == True, "Archived")
        .when(col("is_archived") == False, "Active")
        .otherwise("Unknown")
    )
    
    print(f"  Transformed to {df_dimension.count()} rows")
    print(f"  Final columns: {len(df_dimension.columns)}")
    
    return df_dimension

# =============================================================================
# STEP 3: LOAD - WRITE TO LAKEHOUSE DELTA TABLE
# =============================================================================

def load_to_lakehouse(df, table_name, load_type="overwrite"):
    """
    Load DataFrame to Lakehouse Delta table
    
    Args:
        df: PySpark DataFrame to load
        table_name: Name of the table
        load_type: 'overwrite' or 'merge'
    """
    
    print(f"\nLoading data to Lakehouse...")
    print(f"  Table name: {table_name}")
    print(f"  Load type: {load_type}")
    
    # Extract just the table name from path if provided as path
    if "/" in table_name:
        table_name = table_name.split("/")[-1]
    
    if load_type == "merge":
        # Check if table exists
        table_exists = spark.catalog.tableExists(table_name)
        
        if table_exists:
            print(f"  Performing MERGE (upsert) operation...")
            
            delta_table = DeltaTable.forName(spark, table_name)
            
            # Merge logic: Update existing users, insert new ones
            # Key: user_id (unique)
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.user_id = source.user_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            
            print(f"  Merge completed successfully")
        else:
            print(f"  Table doesn't exist, creating new managed table...")
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            print(f"  Managed table created successfully in metastore")
    
    elif load_type == "overwrite":
        print(f"  Performing OVERWRITE operation...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"  Overwrite completed - managed table created/updated")
    
    # Show sample data
    print(f"\nSample of loaded data:")
    df.show(5, truncate=False)
    
    print(f"\nSchema:")
    df.printSchema()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main execution function - orchestrates the ETL process
    """
    
    try:
        print("="*80)
        print("STARTING USERS DIMENSION ETL")
        print("="*80)
        
        # STEP 1: Extract
        users_data = get_connecteam_users(API_TOKEN)
        
        if not users_data:
            print("WARNING: No users data retrieved. Exiting.")
            return
        
        # Optional: Save raw JSON for audit/debugging
        raw_file_path = f"Files/raw/connecteam_users/{datetime.now().strftime('%Y-%m-%d')}_users_raw.json"
        print(f"\nSaving raw JSON to: {raw_file_path}")
        
        try:
            # Write to Lakehouse Files using mssparkutils
            mssparkutils.fs.put(raw_file_path, json.dumps(users_data, indent=2), True)
            print(f"  ✓ Raw JSON written to Lakehouse: {raw_file_path}")
        except Exception as e:
            print(f"  ⚠ Warning: Could not save raw JSON: {e}")
        
        # STEP 2: Transform
        df_users = transform_users_dimension(users_data)
        
        # STEP 3: Load
        load_to_lakehouse(
            df_users, 
            LAKEHOUSE_TABLE_NAME,
            load_type=LOAD_TYPE
        )
        
        print("\n" + "="*80)
        print("ETL COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"Completion time: {datetime.now()}")
        print(f"Total users processed: {len(users_data)}")
        print(f"Total rows in dimension: {df_users.count()}")
        print("="*80)
        
        # VERIFICATION: Confirm table is properly registered
        print("\n" + "="*80)
        print("TABLE REGISTRATION VERIFICATION")
        print("="*80)
        
        tables = spark.sql("SHOW TABLES").collect()
        table_names = [t.tableName for t in tables]
        
        if LAKEHOUSE_TABLE_NAME in table_names:
            count = spark.sql(f"SELECT COUNT(*) as count FROM {LAKEHOUSE_TABLE_NAME}").collect()[0]['count']
            print(f"\n{LAKEHOUSE_TABLE_NAME}:")
            print(f"  Row count: {count}")
            print(f"  Status: ✅ Registered as managed table")
            
            # Show sample
            print(f"\nSample users:")
            spark.sql(f"""
                SELECT user_id, user_name, email, job_title, department, status
                FROM {LAKEHOUSE_TABLE_NAME}
                LIMIT 5
            """).show(truncate=False)
        else:
            print(f"⚠️  Table {LAKEHOUSE_TABLE_NAME} not found in metastore")
        
        print("\n" + "="*80)
        print("Dimension table is ready for use in:")
        print("  1. Lakehouse UI (Tables → dbo)")
        print("  2. Power BI Desktop (Direct Lake)")
        print("  3. Join to shifts fact table on user_id → assigned_user_id")
        print("="*80)
        
    except Exception as e:
        print("\n" + "="*80)
        print("ETL FAILED")
        print("="*80)
        print(f"Error: {str(e)}")
        print("="*80)
        raise

# Execute
if __name__ == "__main__":
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
