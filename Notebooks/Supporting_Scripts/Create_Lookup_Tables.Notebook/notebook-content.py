# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a84a2c40-9ba8-455c-8106-27db7711294b",
# META       "default_lakehouse_name": "inc_fardap_lakehouse",
# META       "default_lakehouse_workspace_id": "04c5b96c-21ba-4ebb-812e-bed01bbac715",
# META       "known_lakehouses": [
# META         {
# META           "id": "a84a2c40-9ba8-455c-8106-27db7711294b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Create FaRDaP™ Lookup Tables
# 
# ## Purpose
# 
# This notebook fetches **all controlled lists** from the FaRDaP™ API and creates **Delta lookup tables** in the Lakehouse for each one.
# 
# ### What This Notebook Does:
# 1. **Authenticates** with the FaRDaP™ API
# 2. **Fetches all controlled lists** with their complete item data
# 3. **Creates lookup tables** named `LU_{ListTypeName}` for each controlled list
# 4. **Stores in Lakehouse** as managed Delta tables
# 
# ### Lookup Table Naming Convention:
# - `LU_FRSIdListType` → Fire & Rescue Service organisations
# - `LU_IncidentCategoryType` → Types of incidents
# - `LU_PropertyCategoryType` → Building classifications
# - `LU_IncidentCauseType` → Reasons fires started
# - ... and 70+ more!
# 
# ### Prerequisites:
# - FaRDaP™ API credentials configured in Key Vault
# - Variable Library `var_library_fardap` configured
# - Lakehouse attached to this notebook

# MARKDOWN ********************

# ## Step 1: Configuration & Authentication

# CELL ********************

# Import required libraries
import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
from pyspark.sql.functions import col, lit, current_timestamp
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Get Spark session
spark = SparkSession.builder.getOrCreate()

print("✓ Libraries imported successfully")
print(f"✓ Spark version: {spark.version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# FaRDaP™ Configuration
# Get the Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

# Configuration from Fabric variables and Key Vault
API_BASE_URL = vl.getVariable("API_BASE_URL")
KEY_VAULT_URI = vl.getVariable("KEY_VAULT_URI")
USERNAME = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-USERNAME")
PASSWORD = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-PASSWORD")

# Lakehouse configuration
LAKEHOUSE_NAME = "inc_fardap_lakehouse"  # Update if your lakehouse has a different name
SCHEMA_NAME = "dbo"  # Default schema

print(f"✓ Configuration loaded")
print(f"  API Base URL: {API_BASE_URL}")
print(f"  Lakehouse: {LAKEHOUSE_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create session and authenticate
session = requests.Session()

auth_url = f"{API_BASE_URL}/api/v1/auth/init"
auth_payload = {
    "username": USERNAME,
    "password": PASSWORD
}

try:
    print("Authenticating with FaRDaP™ API...\n")
    
    response = session.post(
        auth_url,
        json=auth_payload,
        verify=False,
        timeout=30
    )
    
    if response.status_code == 200:
        auth_response = response.json()
        access_token = auth_response['tokens']['accessToken']
        
        print(f"✓ Authentication successful!")
        print(f"\nReady to fetch controlled lists and create lookup tables!")
    else:
        print(f"✗ Authentication failed: HTTP {response.status_code}")
        print(f"  Response: {response.text}")
        access_token = None
        
except Exception as e:
    print(f"✗ Error during authentication: {str(e)}")
    access_token = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Fetch All Controlled Lists
# 
# Retrieve all controlled lists and their complete item data from the FaRDaP™ API:

# CELL ********************

if not access_token:
    print("✗ Not authenticated. Run the authentication cell first.")
else:
    headers = {'Authorization': f'Bearer {access_token}'}
    all_lists_data = {}  # Store complete data for each list
    all_lists_metadata = []  # Store metadata about each list
    
    try:
        print("Fetching all controlled lists from FaRDaP™ API...\n")
        
        # Get the list of all controlled lists
        list_url = f"{API_BASE_URL}/api/v1/metadata/reference-data/controlled-lists"
        response = session.get(
            list_url,
            headers=headers,
            verify=False,
            timeout=30,
            params={'size': 1000}
        )
        
        if response.status_code == 200:
            page_data = response.json()
            controlled_lists = page_data.get('content', [])
            total = len(controlled_lists)
            
            print(f"Found {total} controlled lists.\n")
            print("Fetching detailed data for each list...\n")
            
            # Fetch complete data for each controlled list
            success_count = 0
            for i, cl in enumerate(controlled_lists):
                full_list_name = cl.get('listName')
                version = cl.get('version')
                
                # Extract the type name from the full list name
                if ' - ' in full_list_name:
                    list_type_name = full_list_name.split(' - ', 1)[1].strip()
                else:
                    list_type_name = full_list_name.strip()
                
                # Fetch full list with all items
                full_list_url = f"{API_BASE_URL}/api/v1/metadata/reference-data/controlled-lists/{list_type_name}/{version}"
                list_response = session.get(
                    full_list_url,
                    headers=headers,
                    verify=False,
                    timeout=30
                )
                
                if list_response.status_code == 200:
                    full_list_data = list_response.json()
                    all_lists_data[list_type_name] = full_list_data
                    
                    items = full_list_data.get('items', [])
                    metadata_entry = {
                        'list_type_name': list_type_name,
                        'display_name': full_list_name,
                        'version': version,
                        'item_count': len(items),
                        'status': 'SUCCESS'
                    }
                    all_lists_metadata.append(metadata_entry)
                    success_count += 1
                    print(f"  [{i+1}/{total}] {list_type_name}: {len(items)} items ✓")
                else:
                    metadata_entry = {
                        'list_type_name': list_type_name,
                        'display_name': full_list_name,
                        'version': version,
                        'item_count': 0,
                        'status': f'HTTP {list_response.status_code}'
                    }
                    all_lists_metadata.append(metadata_entry)
                    print(f"  [{i+1}/{total}] {list_type_name}: FAILED (HTTP {list_response.status_code})")
            
            print(f"\n{'='*80}")
            print(f"✓ Successfully loaded {success_count}/{total} controlled lists!")
            print(f"{'='*80}")
            
        else:
            print(f"✗ Failed to fetch controlled lists: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Create Lookup Tables
# 
# Transform each controlled list into a Delta lookup table in the Lakehouse.
# 
# ### Dynamic Schema Handling:
# The table creation function handles **new/unexpected columns automatically**:
# - Known fields are mapped to standard column names
# - Any new fields from the API are captured with an `extra_` prefix
# - Complex objects/arrays are serialized as JSON strings
# - Schema evolution is enabled - new columns won't break existing tables
# 
# ### Table Schema:
# Each `LU_{ListName}` table will contain:
# - `id` - Unique identifier for the item
# - `name` - Display name
# - `external_id` - External reference ID
# - `obsolete` - Whether the item is deprecated
# - `guidance` - Usage guidance text
# - `keywords` - Search keywords
# - `broader_item_id` - Parent item ID (for hierarchical lists)
# - `broader_item_name` - Parent item name
# - `attributes_json` - JSON array of attributes (if present)
# - `added_in_version` - Version when item was added
# - `last_updated_in_version` - Version when item was last updated
# - `list_version` - Version of the controlled list
# - `loaded_at` - Timestamp when data was loaded
# - `extra_*` - Any additional fields from the API (dynamic)


# CELL ********************

def flatten_item(item: dict, list_version: str) -> dict:
    """
    Flatten a single item from the API, handling any schema dynamically.
    New/unexpected fields are captured automatically - won't break on schema changes.
    
    Args:
        item: Single item from the controlled list
        list_version: Version of the controlled list
    
    Returns:
        Flattened dictionary with all fields as strings (except obsolete)
    """
    row = {}
    
    # Core expected fields (with safe defaults)
    row['id'] = str(item.get('id', '') or '')
    row['name'] = str(item.get('name', '') or '')
    row['external_id'] = str(item.get('externalId', '') or '')
    row['obsolete'] = bool(item.get('obsolete', False))
    row['guidance'] = str(item.get('guidance', '') or '')
    row['keywords'] = str(item.get('keywords', '') or '')
    row['added_in_version'] = str(item.get('addedInVersion', '') or '')
    row['last_updated_in_version'] = str(item.get('lastUpdatedInVersion', '') or '')
    
    # Handle broaderItem (nested object)
    broader_item = item.get('broaderItem', {}) or {}
    row['broader_item_id'] = str(broader_item.get('id', '') or '')
    row['broader_item_name'] = str(broader_item.get('name', '') or '')
    
    # Handle attributes array - flatten to JSON string if present
    attributes = item.get('attributes', [])
    if attributes:
        row['attributes_json'] = json.dumps(attributes)
    else:
        row['attributes_json'] = ''
    
    # Capture ANY additional/new fields dynamically
    # This ensures new columns added to the API won't break the process
    known_fields = {'id', 'name', 'externalId', 'obsolete', 'guidance', 'keywords', 
                    'addedInVersion', 'lastUpdatedInVersion', 'broaderItem', 'attributes'}
    
    for key, value in item.items():
        if key not in known_fields:
            # New/unexpected field - add it with 'extra_' prefix
            safe_key = f"extra_{key.lower()}"
            if isinstance(value, (dict, list)):
                row[safe_key] = json.dumps(value)
            else:
                row[safe_key] = str(value) if value is not None else ''
    
    # Add metadata
    row['list_version'] = list_version
    
    return row


def create_lookup_table(list_type_name: str, list_data: dict) -> bool:
    """
    Create a Delta lookup table from a controlled list.
    Handles dynamic schemas - new columns won't break existing tables.
    
    Args:
        list_type_name: Name of the controlled list (e.g., 'FRSIdListType')
        list_data: Complete list data from the API
    
    Returns:
        True if successful, False otherwise
    """
    try:
        items = list_data.get('items', [])
        list_version = list_data.get('version', 'unknown')
        
        if not items:
            print(f"  ⚠ No items in {list_type_name}, skipping...")
            return False
        
        # Transform items to flat structure (handles dynamic schema)
        rows = [flatten_item(item, list_version) for item in items]
        
        # Get all unique keys across all rows (handles varying schemas per item)
        all_keys = set()
        for row in rows:
            all_keys.update(row.keys())
        
        # Ensure all rows have all keys (fill missing with empty string/False)
        for row in rows:
            for key in all_keys:
                if key not in row:
                    row[key] = False if key == 'obsolete' else ''
        
        # Create DataFrame
        df = spark.createDataFrame(rows)
        
        # Add loaded_at timestamp
        df = df.withColumn('loaded_at', current_timestamp())
        
        # Create table name
        table_name = f"LU_{list_type_name}"
        
        # Write as managed Delta table with schema evolution enabled
        # overwriteSchema=true allows schema changes on overwrite
        # mergeSchema=true would be used for append operations
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error creating {list_type_name}: {str(e)}")
        return False

print("✓ Lookup table creation function defined (with dynamic schema support)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not all_lists_data:
    print("✗ No controlled lists loaded. Run Step 2 first.")
else:
    print(f"Creating lookup tables for {len(all_lists_data)} controlled lists...\n")
    print("="*80)
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    created_tables = []
    failed_tables = []
    
    for i, (list_type_name, list_data) in enumerate(sorted(all_lists_data.items()), 1):
        item_count = len(list_data.get('items', []))
        
        if item_count == 0:
            print(f"[{i}/{len(all_lists_data)}] LU_{list_type_name}: SKIPPED (no items)")
            skipped_count += 1
            continue
        
        if create_lookup_table(list_type_name, list_data):
            print(f"[{i}/{len(all_lists_data)}] LU_{list_type_name}: ✓ Created ({item_count} rows)")
            success_count += 1
            created_tables.append({
                'table_name': f"LU_{list_type_name}",
                'row_count': item_count,
                'version': list_data.get('version', 'unknown')
            })
        else:
            print(f"[{i}/{len(all_lists_data)}] LU_{list_type_name}: ✗ Failed")
            failed_count += 1
            failed_tables.append(list_type_name)
    
    print("\n" + "="*80)
    print("LOOKUP TABLE CREATION SUMMARY")
    print("="*80)
    print(f"\n✓ Successfully created: {success_count} tables")
    print(f"⚠ Skipped (no items):   {skipped_count} tables")
    print(f"✗ Failed:               {failed_count} tables")
    print(f"\nTotal processed:        {len(all_lists_data)} controlled lists")
    
    if failed_tables:
        print(f"\nFailed tables: {', '.join(failed_tables)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Verify Created Tables
# 
# List all lookup tables and verify the data:

# CELL ********************

# List all LU_ tables in the lakehouse
print("Listing all lookup tables in the Lakehouse...\n")
print("="*80)

try:
    # Get all tables
    tables_df = spark.sql("SHOW TABLES")
    
    # Filter for LU_ tables
    lu_tables = tables_df.filter(col("tableName").startswith("LU_") | col("tableName").startswith("lu_"))
    
    lu_table_list = [row.tableName for row in lu_tables.collect()]
    
    print(f"Found {len(lu_table_list)} lookup tables:\n")
    
    # Get row counts for each table
    table_info = []
    for table_name in sorted(lu_table_list):
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0].cnt
            table_info.append({'Table': table_name, 'Rows': count})
        except Exception as e:
            table_info.append({'Table': table_name, 'Rows': f'Error: {str(e)}'})
    
    # Display as DataFrame
    if table_info:
        info_df = spark.createDataFrame(table_info)
        display(info_df)
    else:
        print("No lookup tables found.")
        
except Exception as e:
    print(f"✗ Error listing tables: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preview a specific lookup table
TABLE_TO_PREVIEW = "LU_FRSIdListType"  # Change this to preview different tables

print(f"Preview of {TABLE_TO_PREVIEW}:\n")
print("="*80)

try:
    preview_df = spark.sql(f"SELECT * FROM {TABLE_TO_PREVIEW} LIMIT 20")
    display(preview_df)
    
    total_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {TABLE_TO_PREVIEW}").collect()[0].cnt
    print(f"\nTotal rows in {TABLE_TO_PREVIEW}: {total_count}")
    
except Exception as e:
    print(f"✗ Error previewing table: {str(e)}")
    print(f"\nAvailable tables:")
    for t in sorted(lu_table_list):
        print(f"  - {t}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Create Master Lookup Reference Table
# 
# Create a summary table that documents all available lookup tables:

# CELL ********************

if not all_lists_metadata:
    print("✗ No metadata available. Run Step 2 first.")
else:
    print("Creating master lookup reference table...\n")
    
    # Build reference data
    reference_data = []
    for meta in all_lists_metadata:
        if meta['status'] == 'SUCCESS' and meta['item_count'] > 0:
            reference_data.append({
                'table_name': f"LU_{meta['list_type_name']}",
                'list_type_name': meta['list_type_name'],
                'display_name': meta['display_name'],
                'version': meta['version'],
                'item_count': meta['item_count']
            })
    
    if reference_data:
        # Create DataFrame
        ref_df = spark.createDataFrame(reference_data)
        ref_df = ref_df.withColumn('loaded_at', current_timestamp())
        
        # Save as master reference table
        ref_df.write.format("delta").mode("overwrite").saveAsTable("LU_Master_Reference")
        
        print("✓ Created LU_Master_Reference table")
        print(f"  Total lookup tables documented: {len(reference_data)}\n")
        
        display(ref_df.orderBy('table_name'))
    else:
        print("⚠ No successful lists to document.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# 
# ✅ **Lookup tables have been created in your Lakehouse!**
# 
# ### What Was Created:
# - **Individual Lookup Tables**: `LU_{ListTypeName}` for each controlled list
# - **Master Reference Table**: `LU_Master_Reference` documenting all lookup tables
# 
# ### Common Lookup Tables:
# | Table Name | Description |
# |------------|-------------|
# | `LU_FRSIdListType` | Fire & Rescue Service organisations |
# | `LU_IncidentCategoryType` | Types of incidents |
# | `LU_PropertyCategoryType` | Building classifications |
# | `LU_IncidentCauseType` | Reasons fires started |
# | `LU_StationIdListType` | Fire station identifiers |
# | `LU_ApplianceIdListType` | Appliance/vehicle identifiers |
# 
# ### How to Use:
# ```sql
# -- Join incident data with lookup tables
# SELECT 
#     i.incident_id,
#     i.frs_id,
#     lu.name as frs_name
# FROM incidents i
# LEFT JOIN LU_FRSIdListType lu ON i.frs_id = lu.id
# ```
# 
# ### Re-run This Notebook:
# Run this notebook periodically to refresh lookup tables with the latest controlled list versions from FaRDaP™.

