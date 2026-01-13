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

# Fabric Notebook: Workable Departments Dimension Loader
# Purpose: Load department data from Workable API and create dimension table for Power BI
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Last Updated: 2025-11-13

# %%
# Setup and Configuration

import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, when
from datetime import datetime
import json
from notebookutils import mssparkutils  # Fabric's utility for Key Vault access

# Initialize Spark session
spark = SparkSession.builder.appName("WorkableDepartmentsLoader").getOrCreate()

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
TARGET_TABLE = "dim_workable_departments"

# API endpoint
DEPARTMENTS_ENDPOINT = f"{WORKABLE_API_BASE}/departments"

# %%
# Define API Request Function

def get_workable_departments(api_token):
    """
    Fetch departments from Workable API
    
    Args:
        api_token (str): Workable API bearer token
    
    Returns:
        list: List of department dictionaries
    """
    headers = {
        'Authorization': f'Bearer {api_token}',
        'accept': 'application/json'
    }
    
    try:
        print(f"Fetching departments from: {DEPARTMENTS_ENDPOINT}")
        response = requests.get(DEPARTMENTS_ENDPOINT, headers=headers)
        
        # Check response status
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # The API returns a list directly (not wrapped in a 'departments' key based on your JSON)
        departments = data if isinstance(data, list) else data.get('departments', [])
        
        print(f"✓ Successfully fetched {len(departments)} departments")
        return departments
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching departments: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text}")
        raise

# %%
# Fetch Departments Data

# Fetch departments from API
departments_data = get_workable_departments(WORKABLE_API_TOKEN)

# Display sample data
print("\n" + "="*80)
print("SAMPLE DEPARTMENT DATA:")
print("="*80)
print(json.dumps(departments_data[:3] if len(departments_data) > 0 else [], indent=2))

# %%
# Transform Data for Dimension Table

def transform_departments_data(departments_list):
    """
    Transform raw departments data into dimension table structure
    
    Args:
        departments_list (list): Raw departments data from API
    
    Returns:
        pd.DataFrame: Cleaned and structured DataFrame
    """
    if not departments_list:
        print("⚠ Warning: No departments data to transform")
        return pd.DataFrame()
    
    # Create DataFrame from departments list
    df = pd.DataFrame(departments_list)
    
    # Add metadata columns
    df['etl_loaded_at'] = datetime.now()
    df['etl_source'] = 'workable_api'
    
    # Create hierarchy level based on parent_id
    # Level 0 = top-level departments (parent_id is null)
    # Level 1 = sub-departments (have a parent_id)
    df['hierarchy_level'] = df['parent_id'].apply(lambda x: 0 if pd.isna(x) or x is None else 1)
    
    # Create department type flag
    df['is_parent_department'] = df['parent_id'].isna()
    
    print(f"\n✓ Transformed {len(df)} departments")
    print(f"  - Top-level departments: {df['is_parent_department'].sum()}")
    print(f"  - Sub-departments: {(~df['is_parent_department']).sum()}")
    print(f"  - Sample departments: {df['sample'].sum()}")
    print(f"\nColumns: {', '.join(df.columns.tolist())}")
    
    return df

# %%
# Transform and display the data

# Transform the data
df_pandas = transform_departments_data(departments_data)

# Display the transformed data
print("\n" + "="*80)
print("TRANSFORMED DEPARTMENTS DATA:")
print("="*80)
print(df_pandas)
print(f"\nShape: {df_pandas.shape}")

# Show hierarchy structure
if not df_pandas.empty:
    print("\n" + "="*80)
    print("DEPARTMENT HIERARCHY:")
    print("="*80)
    
    # Show parent departments
    parents = df_pandas[df_pandas['parent_id'].isna()][['id', 'name']]
    print("\nParent Departments:")
    for idx, row in parents.iterrows():
        print(f"  • {row['name']} (id: {row['id']})")
        
        # Show child departments
        children = df_pandas[df_pandas['parent_id'] == row['id']][['id', 'name']]
        if not children.empty:
            for child_idx, child_row in children.iterrows():
                print(f"    └─ {child_row['name']} (id: {child_row['id']})")

# %%
# Create Spark DataFrame with Schema

# Define schema based on Workable API response
department_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("parent_id", StringType(), True),
    StructField("sample", BooleanType(), True),
    StructField("hierarchy_level", IntegerType(), True),
    StructField("is_parent_department", BooleanType(), True),
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
    print("\nAll Departments:")
    verify_df.select("id", "name", "parent_id", "hierarchy_level", "sample") \
        .orderBy("hierarchy_level", "name") \
        .show(100, truncate=False)
    
    print("\n" + "="*80)
    print("SUMMARY STATISTICS:")
    print("="*80)
    
    # Show department counts by hierarchy level
    print("\nDepartments by Hierarchy Level:")
    verify_df.groupBy("hierarchy_level").count().orderBy("hierarchy_level").show()
    
    # Show parent vs child departments
    print("\nDepartment Type Distribution:")
    verify_df.groupBy("is_parent_department").count().show()
    
    # Show sample vs real departments
    print("\nSample vs Real Departments:")
    verify_df.groupBy("sample").count().show()
    
    # Show parent-child relationships
    print("\nParent-Child Relationships:")
    verify_df.alias("child") \
        .join(verify_df.alias("parent"), 
              col("child.parent_id") == col("parent.id"), 
              "left") \
        .select(
            col("parent.name").alias("parent_department"),
            col("child.name").alias("child_department")
        ) \
        .filter(col("parent_department").isNotNull()) \
        .orderBy("parent_department", "child_department") \
        .show(truncate=False)
    
except Exception as e:
    print(f"✗ Error verifying table: {str(e)}")

# %%
# Data Quality Checks

def run_data_quality_checks(table_name):
    """
    Run data quality checks on the departments dimension table
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
    
    # Check 3: Duplicate IDs
    duplicate_count = df.groupBy("id").count().filter(col("count") > 1).count()
    if duplicate_count > 0:
        print(f"\n⚠ Warning: {duplicate_count} duplicate department IDs found")
        df.groupBy("id").count().filter(col("count") > 1).show()
    else:
        print(f"\n✓ No duplicate department IDs")
    
    # Check 4: Orphaned departments (parent_id references non-existent parent)
    orphaned = df.alias("child") \
        .join(df.alias("parent"), 
              (col("child.parent_id") == col("parent.id")) & 
              (col("child.parent_id").isNotNull()), 
              "left_anti")
    
    orphaned_count = orphaned.count()
    if orphaned_count > 0:
        print(f"\n⚠ Warning: {orphaned_count} orphaned departments (parent_id references missing parent)")
        orphaned.select("id", "name", "parent_id").show(truncate=False)
    else:
        print(f"\n✓ No orphaned departments - all parent_id references are valid")
    
    # Check 5: Sample departments
    sample_count = df.filter(col("sample") == True).count()
    if sample_count > 0:
        print(f"\n⚠ Warning: {sample_count} sample departments found")
        df.filter(col("sample") == True).select("id", "name").show(truncate=False)
    else:
        print(f"\n✓ No sample departments")
    
    # Check 6: Empty department names
    empty_names = df.filter((col("name").isNull()) | (col("name") == "")).count()
    if empty_names > 0:
        print(f"\n⚠ Warning: {empty_names} departments with empty names")
    else:
        print(f"\n✓ All departments have names")
    
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
print("WORKABLE DEPARTMENTS LOADER - COMPLETION SUMMARY")
print("="*80)
print(f"✓ API Endpoint: {DEPARTMENTS_ENDPOINT}")
print(f"✓ Target Table: {TARGET_TABLE}")
print(f"✓ Rows Loaded: {df_spark.count() if not df_pandas.empty else 0}")
print(f"✓ Load Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\nTable Structure:")
print("  - id: Unique department identifier")
print("  - name: Department name")
print("  - parent_id: Parent department ID (null for top-level)")
print("  - sample: Flag for sample/test departments")
print("  - hierarchy_level: 0=parent, 1=child")
print("  - is_parent_department: Boolean flag")
print("  - etl_loaded_at: Load timestamp")
print("  - etl_source: Always 'workable_api'")
print("\nNext Steps:")
print("1. Verify table appears in Power BI under 'dbo' schema")
print("2. Create relationships with other Workable tables (jobs, candidates)")
print("3. Use for filtering and grouping in reports")
print("4. Schedule this notebook to run monthly (departments rarely change)")
print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
