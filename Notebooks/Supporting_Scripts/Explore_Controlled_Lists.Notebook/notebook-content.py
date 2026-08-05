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

# # Explore FaRDaP™ Controlled Lists
# 
# ## What are Controlled Lists?
# 
# **Controlled Lists** are reference data that defines valid values for specific fields in FaRDaP™ documents. They act like enumerations or lookup tables — ensuring data consistency across the system.
# 
# ### Examples:
# - **FRSIdListType** → All Fire & Rescue Service organisations
# - **IncidentCategoryType** → Types of incidents (fire, rescue, false alarm, etc.)
# - **PropertyCategoryType** → Building classifications
# - **IncidentCauseType** → Reasons fires started
# 
# This notebook helps you:
# 1. List all available controlled lists
# 2. View the items (values) in each list
# 3. Understand what values are valid for your incident data
# 4. Explore the structure of controlled lists

# CELL ********************

%pip install tabulate

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import required libraries
import requests
import json
from datetime import datetime
import pandas as pd
from tabulate import tabulate
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("✓ Libraries imported successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Configuration & Authentication

# CELL ********************

# FaRDaP™ Configuration
# Get the Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

# Configuration from Fabric variables and Key Vault
API_BASE_URL = vl.getVariable("API_BASE_URL")
KEY_VAULT_URI = vl.getVariable("KEY_VAULT_URI")
USERNAME = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-USERNAME")
PASSWORD = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-PASSWORD")
FRS_ID = vl.getVariable("FRS_ID")

# Create session
session = requests.Session()

# Authenticate
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
        print(f"\nReady to explore controlled lists!")
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

# ## Step 2: Fetch ALL Controlled Lists with ALL Their Items
# 
# Fetch complete data for every controlled list and every item within them:

# CELL ********************

if not access_token:
    print("✗ Not authenticated. Run the authentication cell first.")
else:
    headers = {'Authorization': f'Bearer {access_token}'}
    all_lists_metadata = []  # Store metadata about each list
    all_lists_raw = {}  # Store the full raw response for each list
    
    try:
        print("Fetching ALL controlled lists metadata...\n")
        
        # First, get the list of all controlled lists
        list_url = f"{API_BASE_URL}/api/v1/metadata/reference-data/controlled-lists"
        response = session.get(
            list_url,
            headers=headers,
            verify=False,
            timeout=30,
            params={'size': 100}
        )
        
        if response.status_code == 200:
            page_data = response.json()
            controlled_lists = page_data.get('content', [])
            total = len(controlled_lists)
            
            print(f"Found {total} controlled lists.\n")
            print("Fetching detailed structure for each list...\n")
            
            # For each list, get the full data including all items
            for i, cl in enumerate(controlled_lists):
                full_list_name = cl.get('listName')
                version = cl.get('version')
                
                # Extract the type name from the full list name
                # e.g., "Fire Incident Reference Data List - FRSIdListType" → "FRSIdListType"
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
                    all_lists_raw[list_type_name] = full_list_data
                    
                    # Extract metadata about this list
                    items = full_list_data.get('items', [])
                    list_keys = set(full_list_data.keys())
                    items_keys = set(items[0].keys()) if items else set()
                    
                    metadata_entry = {
                        'list_type_name': list_type_name,
                        'display_name': full_list_name,
                        'version': version,
                        'item_count': len(items),
                        'list_root_keys': sorted(list(list_keys)),
                        'item_keys': sorted(list(items_keys)),
                        'status': 'SUCCESS'
                    }
                    
                    all_lists_metadata.append(metadata_entry)
                    print(f"  [{i+1}/{total}] {list_type_name}: {len(items)} items ✓")
                else:
                    metadata_entry = {
                        'list_type_name': list_type_name,
                        'display_name': full_list_name,
                        'version': version,
                        'item_count': 0,
                        'list_root_keys': [],
                        'item_keys': [],
                        'status': f'HTTP {list_response.status_code}'
                    }
                    all_lists_metadata.append(metadata_entry)
                    print(f"  [{i+1}/{total}] {list_type_name}: HTTP {list_response.status_code}")
            
            success_count = sum(1 for m in all_lists_metadata if m['status'] == 'SUCCESS')
            print(f"\n✓ Successfully loaded {success_count}/{total} controlled lists!")
            
            # Display summary of list structures
            print(f"\n{'='*100}")
            print("CONTROLLED LISTS STRUCTURE SUMMARY")
            print(f"{'='*100}\n")
            
            # Group by item key structure to see variations
            print("ITEM FIELD VARIATIONS:")
            print("-" * 100)
            
            structure_groups = {}
            for meta in all_lists_metadata:
                if meta['status'] == 'SUCCESS':
                    keys_tuple = tuple(sorted(meta['item_keys']))
                    if keys_tuple not in structure_groups:
                        structure_groups[keys_tuple] = []
                    structure_groups[keys_tuple].append(meta['list_type_name'])
            
            for idx, (keys_tuple, list_names) in enumerate(structure_groups.items(), 1):
                print(f"\nStructure {idx}: {len(list_names)} lists with these item fields:")
                print(f"  Fields: {', '.join(keys_tuple)}")
                print(f"  Lists: {', '.join(sorted(list_names)[:5])}", end="")
                if len(list_names) > 5:
                    print(f" ... and {len(list_names) - 5} more")
                else:
                    print()
            
            # Show root-level key variations
            print(f"\n\nROOT-LEVEL OBJECT KEYS:")
            print("-" * 100)
            
            root_groups = {}
            for meta in all_lists_metadata:
                if meta['status'] == 'SUCCESS':
                    keys_tuple = tuple(sorted(meta['list_root_keys']))
                    if keys_tuple not in root_groups:
                        root_groups[keys_tuple] = []
                    root_groups[keys_tuple].append(meta['list_type_name'])
            
            for idx, (keys_tuple, list_names) in enumerate(root_groups.items(), 1):
                print(f"\nRoot Structure {idx}: {len(list_names)} lists")
                print(f"  Keys: {', '.join(keys_tuple)}")
                print(f"  Example list: {list_names[0]}")
            
            # Summary table
            print(f"\n\n{'='*100}")
            print("ALL LISTS SUMMARY TABLE")
            print(f"{'='*100}\n")
            
            table_data = []
            for meta in sorted(all_lists_metadata, key=lambda x: x['list_type_name']):
                table_data.append([
                    meta['list_type_name'],
                    meta['version'],
                    meta['item_count'],
                    len(meta['item_keys']),
                    meta['status']
                ])
            
            print(tabulate(
                table_data,
                headers=['List Type Name', 'Version', 'Item Count', 'Field Count', 'Status'],
                tablefmt='grid'
            ))
            
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

# ## Step 3: Analyze Schema Against OpenAPI Specification
# 
# Understand which lists match the spec and which deviate, and how:

# CELL ********************

# Define the complete schema from OpenAPI specification
# This is what SHOULD be in every Item according to the API spec
OPENAPI_ITEM_SCHEMA = {
    'id': str,
    'name': str,
    'externalId': str,
    'addedInVersion': str,
    'lastUpdatedInVersion': str,
    'broaderItem': dict,  # Contains 'id' and 'name'
    'obsolete': bool,
    'guidance': str,
    'keywords': str,
    'attributes': list,  # Array of {action, subject, value}
}

OPENAPI_CONTROLLEDLIST_SCHEMA = [
    'schemaVersion', 'itemName', 'version', 'versionDate', 'listName',
    'homeLocation', 'latestVersionLocation', 'metadata', 'items'
]

# Analyze deviations
print("="*100)
print("OPENAPI SPECIFICATION ANALYSIS")
print("="*100)
print("\nExpected Item fields (from OpenAPI spec):")
print(f"  {', '.join(sorted(OPENAPI_ITEM_SCHEMA.keys()))}\n")

# Group lists by what they're missing or have extra
missing_by_field = {}
has_extra_fields = {}
perfect_matches = []

for meta in all_lists_metadata:
    if meta['status'] == 'SUCCESS':
        list_name = meta['list_type_name']
        actual_fields = set(meta['item_keys'])
        expected_fields = set(OPENAPI_ITEM_SCHEMA.keys())
        
        missing = expected_fields - actual_fields
        extra = actual_fields - expected_fields
        
        if not missing and not extra:
            perfect_matches.append(list_name)
        else:
            if missing:
                for field in missing:
                    if field not in missing_by_field:
                        missing_by_field[field] = []
                    missing_by_field[field].append(list_name)
            
            if extra:
                for field in extra:
                    if field not in has_extra_fields:
                        has_extra_fields[field] = []
                    has_extra_fields[field].append(list_name)

# Report findings
print(f"\n{'='*100}")
print("SCHEMA DEVIATIONS FROM OPENAPI SPEC")
print(f"{'='*100}\n")

print(f"✓ Perfect matches (exactly match spec): {len(perfect_matches)} lists")
if perfect_matches and len(perfect_matches) <= 5:
    print(f"  {', '.join(perfect_matches)}\n")
elif perfect_matches:
    print(f"  {', '.join(perfect_matches[:5])} ... and {len(perfect_matches)-5} more\n")
else:
    print("  None\n")

if missing_by_field:
    print(f"⚠ Fields MISSING from spec (present in some lists):")
    for field in sorted(missing_by_field.keys()):
        count = len(missing_by_field[field])
        print(f"  - {field}: missing from {count} lists")
        if count <= 3:
            print(f"      Lists: {', '.join(missing_by_field[field])}")
    print()

if has_extra_fields:
    print(f"⚠ Fields EXTRA (not in spec but found in some lists):")
    for field in sorted(has_extra_fields.keys()):
        count = len(has_extra_fields[field])
        print(f"  - {field}: present in {count} lists")
        if count <= 3:
            print(f"      Lists: {', '.join(has_extra_fields[field])}")
    print()

# Detailed analysis table
print(f"\n{'='*100}")
print("DETAILED FIELD COVERAGE")
print(f"{'='*100}\n")

field_coverage = {}
for field in OPENAPI_ITEM_SCHEMA.keys():
    field_coverage[field] = {
        'present_in': 0,
        'missing_from': 0
    }

for meta in all_lists_metadata:
    if meta['status'] == 'SUCCESS':
        for field in OPENAPI_ITEM_SCHEMA.keys():
            if field in meta['item_keys']:
                field_coverage[field]['present_in'] += 1
            else:
                field_coverage[field]['missing_from'] += 1

coverage_data = []
for field in sorted(OPENAPI_ITEM_SCHEMA.keys()):
    present = field_coverage[field]['present_in']
    missing = field_coverage[field]['missing_from']
    pct = (present / (present + missing) * 100) if (present + missing) > 0 else 0
    coverage_data.append([field, present, missing, f"{pct:.1f}%"])

print(tabulate(
    coverage_data,
    headers=['Field', 'Present In', 'Missing From', 'Coverage %'],
    tablefmt='grid'
))

print(f"\n✓ Analysis complete. Raw data in `all_lists_raw` and metadata in `all_lists_metadata`")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Export All Lists to CSV
# 
# Save all controlled lists and their items to CSV files (one file per list):

# CELL ********************

import os
from pathlib import Path

if not all_lists_data:
    print("⚠ No controlled lists loaded. Run Step 2 first.")
else:
    export_dir = "./fardap_controlled_lists"
    os.makedirs(export_dir, exist_ok=True)
    
    print(f"Exporting all controlled lists to: {export_dir}\n")
    
    # Create master file with all data
    master_data = []
    
    for list_name, list_data in all_lists_data.items():
        version = list_data.get('version')
        category = list_data.get('metadata', {}).get('category', 'N/A')
        
        items = list_data.get('items', [])
        for item in items:
            master_data.append({
                'List_Name': list_name,
                'Version': version,
                'Category': category,
                'Item_ID': item.get('id'),
                'Item_Name': item.get('name'),
                'External_ID': item.get('externalId'),
                'Obsolete': item.get('obsolete', False),
                'Guidance': item.get('guidance', ''),
                'Keywords': item.get('keywords', '')
            })
            
            # Also save individual list files
            list_export_path = f"{export_dir}/{list_name}.csv"
            
    # Save master file
    if master_data:
        master_df = pd.DataFrame(master_data)
        master_file = f"{export_dir}/ALL_CONTROLLED_LISTS.csv"
        master_df.to_csv(master_file, index=False)
        print(f"✓ Master file: {master_file}")
        print(f"  Total rows: {len(master_df)}\n")
    
    # Save individual list files
    for list_name, list_data in all_lists_data.items():
        items = list_data.get('items', [])
        
        if items:
            list_export_path = f"{export_dir}/{list_name}.csv"
            
            list_data_for_export = []
            for item in items:
                list_data_for_export.append({
                    'ID': item.get('id'),
                    'Name': item.get('name'),
                    'External_ID': item.get('externalId'),
                    'Obsolete': item.get('obsolete', False),
                    'Added_Version': item.get('addedInVersion'),
                    'Updated_Version': item.get('lastUpdatedInVersion'),
                    'Guidance': item.get('guidance', ''),
                    'Keywords': item.get('keywords', '')
                })
            
            df = pd.DataFrame(list_data_for_export)
            df.to_csv(list_export_path, index=False)
    
    print(f"✓ Exported {len(all_lists_data)} individual list files")
    print(f"\nFiles created in: {export_dir}/")
    
    # List files
    print("\nFiles:")
    for f in sorted(os.listdir(export_dir)):
        file_path = os.path.join(export_dir, f)
        file_size = os.path.getsize(file_path)
        print(f"  - {f} ({file_size:,} bytes)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: View Any Controlled List Items
# 
# Select a specific list to view all its items:

# CELL ********************

# View items from a specific list
LIST_TO_VIEW = "StationIdListType"  # Change this to view different lists

if not all_lists_data:
    print("⚠ No controlled lists loaded. Run Step 2 first.")
elif LIST_TO_VIEW not in all_lists_data:
    print(f"✗ List '{LIST_TO_VIEW}' not found")
    print(f"\nAvailable lists ({len(all_lists_data)}):")
    for name in sorted(all_lists_data.keys()):
        item_count = len(all_lists_data[name].get('items', []))
        print(f"  - {name} ({item_count} items)")
else:
    list_data = all_lists_data[LIST_TO_VIEW]
    items = list_data.get('items', [])
    
    print(f"\n{'='*70}")
    print(f"LIST: {LIST_TO_VIEW}")
    print(f"{'='*70}")
    print(f"Version: {list_data.get('version')}")
    print(f"Category: {list_data.get('metadata', {}).get('category', 'N/A')}")
    print(f"Total Items: {len(items)}\n")
    
    if items:
        table_data = []
        for item in items:
            table_data.append([
                item.get('id'),
                item.get('name'),
                'Yes' if item.get('obsolete') else 'No',
                item.get('guidance', '')[:40] if item.get('guidance') else ''
            ])
        
        print(tabulate(
            table_data,
            headers=['ID', 'Name', 'Obsolete', 'Guidance (Preview)'],
            tablefmt='grid',
            maxcolwidths=[10, 30, 10, 30]
        ))
    else:
        print("⚠ No items found in this list")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Search All Lists
# 
# Search for a term across all controlled lists and all items:

# CELL ********************

# Search across ALL controlled lists and ALL items
SEARCH_TERM = "fire"  # Change to search for different terms

if not all_lists_data:
    print("⚠ No controlled lists loaded. Run Step 2 first.")
else:
    print(f"Searching for '{SEARCH_TERM}' across all lists and all items...\n")
    
    results = []
    search_lower = SEARCH_TERM.lower()
    
    for list_name, list_data in all_lists_data.items():
        items = list_data.get('items', [])
        
        for item in items:
            item_name = str(item.get('name', '')).lower()
            item_id = str(item.get('id', '')).lower()
            guidance = str(item.get('guidance', '')).lower()
            
            if (search_lower in item_name or 
                search_lower in item_id or 
                search_lower in guidance):
                results.append({
                    'list': list_name,
                    'id': item.get('id'),
                    'name': item.get('name'),
                    'obsolete': item.get('obsolete', False)
                })
    
    if results:
        print(f"✓ Found {len(results)} matches:\n")
        
        table_data = []
        for result in results:
            table_data.append([
                result['list'],
                result['id'],
                result['name'],
                'Yes' if result['obsolete'] else 'No'
            ])
        
        print(tabulate(
            table_data,
            headers=['List Name', 'Item ID', 'Item Name', 'Obsolete'],
            tablefmt='grid'
        ))
    else:
        print(f"⚠ No matches found for '{SEARCH_TERM}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# 
# ✅ You now have complete reference data for all FaRDaP™ controlled lists!
# 
# ### What You Got:
# - **Step 2:** All controlled lists loaded into memory with complete items
# - **Step 3:** CSV exports (master file + individual files for each list)
# - **Step 4:** View any specific list's items
# - **Step 5:** Search across all lists instantly
# 
# ### How to Use This Data:
# 1. **Data Validation:** Check if incident values match controlled list IDs
# 2. **Documentation:** Reference CSVs show all valid values
# 3. **Analysis:** Understand incident categorization codes
# 4. **Mapping:** Bridge between system IDs and human-readable names
# 
# ### CSV Files Created:
# - `ALL_CONTROLLED_LISTS.csv` - Master file with all items from all lists
# - `FRSIdListType.csv` - FRS organisations
# - `IncidentCategoryType.csv` - Incident types
# - `PropertyCategoryType.csv` - Property types
# - ... and 70+ more!
