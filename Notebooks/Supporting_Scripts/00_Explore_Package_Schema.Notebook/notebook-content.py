# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 00 - Explore Package Schema & Data Structure
# 
# ## Understand the Incident Data Model
# 
# This notebook explores the FaRDaP™ API schema to understand:
# - What packages/schemas are available
# - What fields/questions exist for Incident data
# - The structure of the Incident form
# - What all 200+ possible questions are
# 
# **Purpose:**
# - Discover the complete data model before fetching incidents
# - Understand what questions/fields are in the Incident form
# - Plan the transformation strategy
# 
# **Expected Duration:** 5-10 minutes
# 
# ⚠️ **Run BEFORE 01_Bronze_Bulk_Load.ipynb**

# MARKDOWN ********************

# ## Step 1: Configuration & Authentication

# CELL ********************

import requests
import json
import pandas as pd
from datetime import datetime
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# FaRDaP™ API Configuration
# Get the Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

# Configuration from Fabric variables and Key Vault
API_BASE_URL = vl.getVariable("API_BASE_URL")
KEY_VAULT_URI = vl.getVariable("KEY_VAULT_URI")
USERNAME = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-USERNAME")
PASSWORD = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-PASSWORD")
FRS_ID = vl.getVariable("FRS_ID")

print(f"API Base URL: {API_BASE_URL}")
print(f"FRS ID: {FRS_ID}")
print()

# Authenticate
auth_url = f"{API_BASE_URL}/api/v1/auth/init"

try:
    print(f"Authenticating with {auth_url}...")
    response = requests.post(
        auth_url,
        json={"username": USERNAME, "password": PASSWORD},
        verify=False,
        timeout=30
    )
    
    if response.status_code == 200:
        auth_data = response.json()
        access_token = auth_data.get('tokens', {}).get('accessToken')
        print(f"✓ Authentication successful")
    else:
        print(f"✗ Authentication failed: {response.status_code}")
        raise Exception(f"Auth failed: {response.status_code}")
        
except Exception as e:
    print(f"✗ Error: {e}")
    raise

# Set up session
session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
})

print("✓ Session configured")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: List Available Packages

# CELL ********************

# Get list of available packages
packages_url = f"{API_BASE_URL}/api/v1/metadata/reference-data/packages"

print(f"Fetching packages from {packages_url}...")
print()

response = session.get(packages_url, verify=False, timeout=30)

if response.status_code == 200:
    packages_data = response.json()
    packages = packages_data.get('content', [])
    
    print(f"✓ Found {len(packages)} packages:")
    print()
    
    for pkg in packages:
        print(f"  Package: {pkg.get('name')}")
        print(f"    Version: {pkg.get('version')}")
        print(f"    Build Time: {pkg.get('buildTime')}")
        print(f"    Entries: {len(pkg.get('entries', []))}")
        print()
else:
    print(f"✗ Error: {response.status_code}")
    print(response.text[:500])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Get Incident Package Details

# CELL ********************

# The packages don't contain entries - the schema is in controlled lists
# Let's adapt and explore the controlled lists instead

print("ℹ️  Note: Packages have 0 entries - schema is likely in controlled lists")
print()
print("Available packages:")
for pkg in packages:
    print(f"  - {pkg.get('name')} v{pkg.get('version')}")
    print(f"    Build Time: {pkg.get('buildTime')}")
print()
print("✓ Moving to Step 4 to explore controlled lists for Incident fields...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Explore Controlled Lists Available

# CELL ********************

# Get controlled lists - fetch all pages to see complete schema
lists_url = f"{API_BASE_URL}/api/v1/metadata/reference-data/controlled-lists"

print(f"Fetching controlled lists...")
print()

all_lists = []
page = 0
page_size = 100

while True:
    response = session.get(f"{lists_url}?page={page}&size={page_size}", verify=False, timeout=30)
    
    if response.status_code == 200:
        lists_data = response.json()
        current_page_lists = lists_data.get('content', [])
        
        if not current_page_lists:
            break
            
        all_lists.extend(current_page_lists)
        page += 1
        print(f"  Fetched page {page}: {len(current_page_lists)} lists (total: {len(all_lists)})")
    else:
        print(f"✗ Error on page {page}: {response.status_code}")
        break

print()
print(f"✓ Total controlled lists: {len(all_lists)}")
print()

# Find Incident-related lists
incident_lists = [lst for lst in all_lists if 'incident' in lst.get('listName', '').lower()]

if incident_lists:
    print(f"✓ Found {len(incident_lists)} Incident-related lists:")
    print()
    for lst in incident_lists[:30]:
        list_name = lst.get('listName')
        item_count = len(lst.get('items', []))
        print(f"  - {list_name}")
        print(f"    Items: {item_count}")
        if item_count <= 10:
            items = lst.get('items', [])
            for item in items:
                print(f"      • {item.get('itemCode')}: {item.get('itemName')}")
        print()
else:
    print(f"No Incident-specific lists found. All {len(all_lists)} lists:")
    print()
    for i, lst in enumerate(all_lists[:50], 1):
        list_name = lst.get('listName')
        item_count = len(lst.get('items', []))
        print(f"  {i}. {list_name} ({item_count} items)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Get Sample Incident Schema

# CELL ********************

# Try to get schema from a sample incident document itself
# The API endpoint might have a schema or metadata endpoint

print(f"Looking for schema/metadata endpoints...")
print()

# Common endpoints to try
endpoints_to_try = [
    f"{API_BASE_URL}/api/v1/metadata/incident/schema",
    f"{API_BASE_URL}/api/v1/document-schema",
    f"{API_BASE_URL}/api/v1/metadata/document/Incident",
]

for endpoint in endpoints_to_try:
    print(f"  Trying: {endpoint}")
    try:
        response = session.get(endpoint, verify=False, timeout=10)
        if response.status_code == 200:
            print(f"    ✓ Success!")
            schema = response.json()
            print(json.dumps(schema, indent=2)[:1000])
            break
        else:
            print(f"    ✗ {response.status_code}")
    except:
        print(f"    ✗ Connection error")

print()
print("If all fail, we'll examine the sample incident structure in next step...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6: Sample an Incident to See Structure

# CELL ********************

# Fetch a small sample of incidents to see their structure
search_url = f"{API_BASE_URL}/api/v1/document/search"

search_payload = {
    "query": {
        "list": {"documentTypes": ["Incident"]},
        "match": {"territoryFrsId": FRS_ID}
    },
    "cursor": {"size": 50, "lastDocumentValues": []}
}

print(f"Fetching sample incident...")
print()

response = session.post(search_url, json=search_payload, verify=False, timeout=30)

if response.status_code == 200:
    data = response.json()
    results = data.get('results', [])
    
    if results:
        incident = results[0]
        print(f"✓ Sample incident fetched")
        print()
        
        # Show top-level structure
        print(f"Top-level fields:")
        for key in incident.keys():
            print(f"  - {key}")
        
        print()
        print(f"Content structure (questions/fields):")
        content = incident.get('content', {})
        
        def print_structure(obj, indent=0):
            prefix = "  " * indent
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, dict):
                        print(f"{prefix}- {key} (object)")
                        print_structure(value, indent)
                    elif isinstance(value, list):
                        print(f"{prefix}- {key} (array, {len(value)} items)")
                    else:
                        print(f"{prefix}- {key}: {value}")
        
        print_structure(content)
        
        print()
        print(f"Full incident JSON:")
        print(json.dumps(incident, indent=2))
    else:
        print("✗ No incidents found")
else:
    print(f"✗ Error: {response.status_code}")
    print(response.text[:5000000])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# 
# This notebook explored:
# - Available packages in the FaRDaP™ API
# - The Incident schema with all fields/questions
# - The structure of actual incident data
# 
# **Next Steps:**
# - Use findings to understand data structure for transformation
# - Proceed with 01_Bronze_Bulk_Load.ipynb to fetch all incidents
