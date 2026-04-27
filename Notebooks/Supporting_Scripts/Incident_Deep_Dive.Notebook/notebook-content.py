# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Incident Deep Dive - Complete Data Extraction
# 
# ## Fetch & Display ABSOLUTELY EVERYTHING for a Single Incident
# 
# This notebook fetches one specific incident and displays every single piece of data available.
# 
# **Target Incident:**
# - FRS Incident Number: 53018009
# - Document ID: 13225834
# 
# **What You'll Get:**
# - Complete incident JSON structure
# - All metadata and properties
# - All content/questions with answers
# - All nested objects and arrays
# - All comments, text boxes, and supplementary data
# - Formatted display of everything
# 
# **Duration:** 2-5 minutes

# MARKDOWN ********************

# ## Step 1: Configuration & Authentication

# CELL ********************

import requests
import json
import pandas as pd
from datetime import datetime
import urllib3
from collections import defaultdict
from pprint import pprint

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

# Target incident
TARGET_FRS_INCIDENT_NUMBER = "53018009"
TARGET_DOCUMENT_ID = "13225834"

print(f"Target Incident:")
print(f"  FRS Incident Number: {TARGET_FRS_INCIDENT_NUMBER}")
print(f"  Document ID: {TARGET_DOCUMENT_ID}")
print()
print(f"API Configuration:")
print(f"  Base URL: {API_BASE_URL}")
print(f"  FRS ID: {FRS_ID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Authenticate

# CELL ********************

# Authenticate with API
auth_url = f"{API_BASE_URL}/api/v1/auth/init"

try:
    print(f"Authenticating...")
    response = requests.post(
        auth_url,
        json={
            "username": USERNAME,
            "password": PASSWORD
        },
        verify=False,
        timeout=30
    )
    
    if response.status_code == 200:
        auth_data = response.json()
        access_token = auth_data.get('tokens', {}).get('accessToken')
        print(f"✓ Authentication successful")
    else:
        print(f"✗ Authentication failed: {response.status_code}")
        print(response.text[:500])
        raise Exception(f"Auth failed")

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

# ## Step 3: Fetch Incident by Document ID

# CELL ********************

# Fetch specific incident using direct GET endpoint
doc_url = f"{API_BASE_URL}/api/v1/document/{TARGET_DOCUMENT_ID}"

# Optional: add frsId query parameter if needed
params = {}
if FRS_ID:
    params['frsId'] = FRS_ID

print(f"Fetching incident...")
print(f"  Document ID: {TARGET_DOCUMENT_ID}")
print(f"  Endpoint: GET {doc_url}")
print()

response = session.get(doc_url, params=params, verify=False, timeout=30)

if response.status_code == 200:
    incident = response.json()
    print(f"✓ Incident retrieved successfully!")
    print()
else:
    print(f"✗ API Error: {response.status_code}")
    print(response.text[:500])
    raise Exception(f"API error: {response.status_code}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Display Complete Top-Level Structure

# CELL ********************

# Display top-level keys
print(f"Top-level Incident Structure:")
print(f"=================================")
print()

for key in sorted(incident.keys()):
    value = incident[key]
    if isinstance(value, dict):
        print(f"Key: {key}")
        print(f"  Value: [object with {len(value)} keys]")
        print(json.dumps(value, indent=2))
    elif isinstance(value, list):
        print(f"Key: {key}")
        print(f"  Value: [array with {len(value)} items]")
        print(json.dumps(value, indent=2))
    else:
        print(f"Key: {key}")
        print(f"  Value: {value}")
    
    print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Extract & Display All Properties

# CELL ********************

# Get properties
properties = incident.get('properties', {})

print(f"Properties:")
print(f"=================================")
print()

if properties:
    for key, value in sorted(properties.items()):
        if isinstance(value, (dict, list)):
            print(f"{key}:")
            print(json.dumps(value, indent=2))
        else:
            print(f"{key}: {value}")
        print()
else:
    print("No properties found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6: Recursively Extract ALL Content Questions & Answers

# CELL ********************

def flatten_content(obj, path="", result=None):
    """
    Completely flatten content structure to show every single value.
    Returns flat dict with full paths and ALL values.
    """
    if result is None:
        result = {}
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            current_path = f"{path}.{key}" if path else key
            
            if isinstance(value, dict):
                flatten_content(value, current_path, result)
            elif isinstance(value, list):
                if len(value) == 0:
                    result[current_path] = []
                elif isinstance(value[0], dict):
                    # Store array itself
                    result[current_path] = value
                    # Also flatten each item
                    for i, item in enumerate(value):
                        flatten_content(item, f"{current_path}[{i}]", result)
                else:
                    result[current_path] = value
            else:
                result[current_path] = value
    
    return result

# Extract all content
content = incident.get('content', {})

print(f"Content - Flattened Q&A Pairs:")
print(f"=================================")
print()

if content:
    flattened = flatten_content(content)
    
    print(f"Total questions/fields found: {len(flattened)}")
    print()
    
    # Sort and display - SHOW EVERYTHING, NO LIMITS
    for i, (question, answer) in enumerate(sorted(flattened.items()), 1):
        print(f"{i}. Question/Field: {question}")
        
        # Format answer based on type
        if isinstance(answer, dict):
            print(f"   Answer: [nested object]")
            print(json.dumps(answer, indent=6))
        elif isinstance(answer, list):
            print(f"   Answer: [array with {len(answer)} items]")
            print(json.dumps(answer, indent=6))
        else:
            print(f"   Answer: {answer}")
        print()
else:
    print("No content found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7: Display Complete Raw JSON

# CELL ********************

# Display full JSON
print(f"Complete Incident JSON (Raw):")
print(f"=================================")
print()

full_json = json.dumps(incident, indent=2)
print(full_json)

print()
print(f"Total JSON size: {len(full_json)} bytes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 8: Create Summary Report

# CELL ********************

# Create summary
doc_id = incident.get('properties', {}).get('documentId', 'UNKNOWN')
doc_type = incident.get('documentType', 'UNKNOWN')
dcd_version = incident.get('dcdVersion', 'UNKNOWN')
validation_status = incident.get('validationStatus', 'UNKNOWN')

print("="*70)
print("INCIDENT DEEP DIVE SUMMARY")
print("="*70)
print()
print(f"Incident Details:")
print(f"  Document ID: {doc_id}")
print(f"  Document Type: {doc_type}")
print(f"  DCD Version: {dcd_version}")
print(f"  Validation Status: {validation_status}")
print()
print(f"Data Structure:")
print(f"  Top-level keys: {list(incident.keys())}")
print(f"  Properties count: {len(incident.get('properties', {}))}")
print(f"  Content questions/fields: {len(flattened) if content else 0}")
print(f"  Total JSON size: {len(full_json)} bytes")
print()
print(f"Key Metadata:")
props = incident.get('properties', {})
print(f"  Created: {props.get('dateCreated', 'N/A')}")
print(f"  Updated: {props.get('dateUpdated', 'N/A')}")
print(f"  Version: {props.get('version', 'N/A')}")
print()
print(f"Content Questions/Fields (total: {len(flattened)}):") 
for i, (question, answer) in enumerate(sorted(flattened.items()), 1):
    if isinstance(answer, (dict, list)):
        a_str = f"[{type(answer).__name__}]"
    else:
        a_str = str(answer)
    print(f"  {i}. {question}: {a_str}")
print()
print("="*70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 9: Export to File (Optional)

# CELL ********************

# Save complete incident JSON to file
import os

export_path = f"/tmp/incident_{TARGET_DOCUMENT_ID}_full.json"

try:
    with open(export_path, 'w') as f:
        json.dump(incident, f, indent=2)
    
    print(f"✓ Incident exported to: {export_path}")
    print(f"  File size: {os.path.getsize(export_path)} bytes")
except Exception as e:
    print(f"✗ Error exporting: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
