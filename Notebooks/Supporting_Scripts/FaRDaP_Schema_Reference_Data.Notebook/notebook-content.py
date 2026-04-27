# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # FaRDaP Reference Data Schema Metadata
# Fetch schema definitions with display names and field mappings for incident fields

# MARKDOWN ********************

# ## Import Required Libraries

# CELL ********************

import requests
import json
import pandas as pd
from datetime import datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

get_test = vl.getVariable("API_BASE_URL")

print(get_test)

KEY_VAULT_URI = vl.getVariable("KEY_VAULT_URI")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set Up API Authentication

# CELL ********************

# Get credentials from variable library (if in Fabric)

vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")
API_BASE_URL = vl.getVariable("API_BASE_URL")
KEY_VAULT_URI = vl.getVariable("KEY_VAULT_URI")
USERNAME = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-USERNAME")
PASSWORD = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-PASSWORD")


# Authenticate
auth_url = f'{API_BASE_URL}/api/v1/auth/init'
auth_payload = {'username': USERNAME, 'password': PASSWORD}
auth_resp = requests.post(auth_url, json=auth_payload, verify=False, timeout=30)
auth_resp.raise_for_status()

access_token = auth_resp.json().get('tokens', {}).get('accessToken')
headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}

print(f'Authenticated successfully. Token acquired.')
print(f'API Base URL: {API_BASE_URL}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The field display names must be in the document structure itself
# Let's explore the full document to find where labels like "Q10.4 General Notes" are stored

try:
    FRS_ID = vl.getVariable("FRS_ID")
except:
    FRS_ID = "17"  # Use the FRS from your example

# Search for a sample incident
search_url = f'{API_BASE_URL}/api/v1/document/search'
search_payload = {
    'query': {
        'list': {'documentTypes': ['Incident']},
        'match': {'territoryFrsId': str(FRS_ID)}
    },
    'cursor': {'size': 1}
}

search_resp = requests.post(search_url, json=search_payload, headers=headers, verify=False, timeout=60)
search_resp.raise_for_status()
search_results = search_resp.json()

results = search_results.get('results', [])
if results:
    sample_incident = results[0]
    doc_id = sample_incident.get('properties', {}).get('documentId')
    
    # Fetch the full document
    doc_url = f'{API_BASE_URL}/api/v1/document/{doc_id}'
    doc_resp = requests.get(doc_url, params={'frsId': FRS_ID}, headers=headers, verify=False, timeout=60)
    doc_resp.raise_for_status()
    full_doc = doc_resp.json()
    
    print(f"Retrieved Document ID: {doc_id}")
    print(f"Document Type: {full_doc.get('documentType')}")
    print(f"DCD Version: {full_doc.get('dcdVersion')}")
    
    # Save the full document structure to inspect
    print("\n=== FULL DOCUMENT STRUCTURE ===\n")
    print(json.dumps(full_doc, indent=2))
    
    print("\n\n=== EXPLORING CONTENT STRUCTURE ===\n")
    content = full_doc.get('content', {})
    
    if isinstance(content, dict):
        print(f"Content has {len(content)} top-level keys:")
        for key in list(content.keys()):
            val = content[key]
            print(f"  {key}: {type(val).__name__}")
            
            # Look for nested structures that might contain labels
            if isinstance(val, dict):
                sample_keys = list(val.keys())[:3]
                print(f"    Sample nested keys: {sample_keys}")
            elif isinstance(val, list) and val and isinstance(val[0], dict):
                print(f"    List of dicts, first item keys: {list(val[0].keys())[:3]}")
    
    # Look for any field that might contain "Q10" or "question" patterns
    print("\n\n=== SEARCHING FOR QUESTION/LABEL PATTERNS ===\n")
    full_json_str = json.dumps(full_doc)
    if '"Q10' in full_json_str or '"question' in full_json_str.lower():
        print("Found question-like patterns in document!")
        # Show locations
        import re
        q_patterns = re.findall(r'"Q\d+[^"]*"[^}]{0,200}', full_json_str)[:5]
        for p in q_patterns:
            print(p[:200])
    else:
        print("No question patterns found in document structure")
        
else:
    print("No incidents found - check FRS_ID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The field display names come from the DCD (Data Collection Document) schema definition
# We need to fetch the schema for the Incident document type

# First, let's explore what's available in the packages
print("=== EXPLORING REFERENCE DATA PACKAGES ===\n")

packages = packages_data.get('content', [])
if packages:
    # Get the latest package
    latest_pkg = packages[0]
    pkg_name = latest_pkg.get('name')
    pkg_version = latest_pkg.get('version')
    
    print(f"Latest Package: {pkg_name} v{pkg_version}")
    print(f"Entries: {latest_pkg.get('entries', [])}")
    
    # Fetch the full package with entries
    manifest_url = f'{API_BASE_URL}/api/v1/metadata/reference-data/packages/{pkg_name}/{pkg_version}'
    manifest_resp = requests.get(manifest_url, headers=headers, verify=False, timeout=60)
    manifest_resp.raise_for_status()
    manifest_data = manifest_resp.json()
    
    entries = manifest_data.get('entries', [])
    print(f"\nPackage has {len(entries)} entries:")
    
    # Look for Incident-related schemas
    incident_entries = []
    for entry in entries:
        item_name = entry.get('itemName', '')
        list_name = entry.get('listName', '')
        schema_version = entry.get('schemaVersion', '')
        
        print(f"  - Item: {item_name}")
        print(f"    List: {list_name}")
        print(f"    Schema: {schema_version}")
        print()
        
        if 'incident' in item_name.lower() or 'incident' in list_name.lower():
            incident_entries.append(entry)
    
    if incident_entries:
        print(f"\n=== FOUND {len(incident_entries)} INCIDENT-RELATED ENTRIES ===\n")
        for entry in incident_entries:
            print(json.dumps(entry, indent=2))
    
    # Try fetching an Incident schema if we found one
    print("\n\n=== ATTEMPTING TO FETCH INCIDENT SCHEMA ===\n")
    
    # Try common schema names
    schema_attempts = [
        'IncidentType',
        'Incident',
        'IncidentSchema',
        'IRS1',
        'IRS',
        'incident-1.0.0'
    ]
    
    for schema_name in schema_attempts:
        try:
            schema_url = f'{API_BASE_URL}/api/v1/metadata/reference-data/controlled-lists/{schema_name}/latest'
            schema_resp = requests.get(schema_url, headers=headers, verify=False, timeout=60)
            
            if schema_resp.status_code == 200:
                schema_data = schema_resp.json()
                print(f"\nSuccessfully fetched schema: {schema_name}")
                print(f"Structure: {json.dumps(schema_data, indent=2)[:1000]}")
                break
        except:
            continue
    
    print("\n\nNote: The schema definition with field labels may be in a separate endpoint")
    print("or embedded in documentation. Check if there's a schema definition API endpoint.")
else:
    print("No packages found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CONCLUSION: Field display names (like "Q10.4 General Notes") are NOT available via the FaRDaP API
# 
# The API provides:
# - Controlled Lists: Lookup values for dropdown fields (IncidentType, PropertyType, etc.)
# - Document content: Technical field names (additionalInfo, incidentOnAttendance, etc.)
#
# Field display names come from the IRS1 (Incident Recording System) form specification,
# which is external documentation not exposed through the API.
#
# SOLUTION: Create a manual mapping dictionary based on IRS1 form documentation

print("=== FIELD DISPLAY NAME MAPPING ===\n")
print("Field labels must be manually mapped from IRS1 form documentation.")
print("\nExample structure for creating the mapping:\n")

# Example mapping structure (populate with actual IRS1 form field labels)
IRS1_FIELD_LABELS = {
    # Section 10 - Additional Information
    'content.additionalInfo': 'Q10.4 General Notes',
    
    # Section 5 - Fire Details
    'content.incidentOnAttendance.fire.cause': 'Q5.1 Cause of Fire',
    'content.incidentOnAttendance.fire.primaryFire.dwelling.fireLocation': 'Q5.2 Fire Location',
    
    # Section 3 - Incident Details
    'content.incidentAtCall.mobiliseIncidentType': 'Q3.1 Incident Type',
    'content.incidentAtCall.timeOfCall': 'Q3.2 Time of Call',
    
    # Add more mappings based on IRS1 form sections...
    # Section 1: Incident Identification
    # Section 2: Location
    # Section 4: Resources
    # Section 6: Casualties
    # Section 7: Property Damage
    # etc.
}

print("Created template with sample mappings.")
print(f"Total fields mapped: {len(IRS1_FIELD_LABELS)}")
print("\nTo complete the mapping:")
print("1. Obtain IRS1 form specification or data dictionary")
print("2. Match each technical field name to its question label")
print("3. Update IRS1_FIELD_LABELS dictionary")
print("4. Use in full load notebook to enrich incident data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Retrieve Reference Data Packages

# CELL ********************

# Fetch available reference data packages
packages_url = f'{API_BASE_URL}/api/v1/metadata/reference-data/packages'
packages_resp = requests.get(packages_url, headers=headers, verify=False, timeout=60)
packages_resp.raise_for_status()
packages_data = packages_resp.json()

# Display package information
packages = packages_data.get('content', [])
print(f'Found {len(packages)} Reference Data packages:\n')

for pkg in packages:
    print(f"Name: {pkg.get('name')}")
    print(f"  Version: {pkg.get('version')}")
    print(f"  Built: {pkg.get('buildTime')}")
    print(f"  Entries: {len(pkg.get('entries', []))}")
    print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fetch Latest Incident Schema Package

# CELL ********************

# Explore available packages and their structure
# The package structure might be different - let's inspect what's available

print("Inspecting package structure...\n")

# Get the latest version of the main reference data package
packages = packages_data.get('content', [])
if packages:
    latest_pkg = packages[0]  # Get most recent version
    pkg_name = latest_pkg.get('name')
    pkg_version = latest_pkg.get('version')
    
    print(f"Using package: {pkg_name} v{pkg_version}")
    
    # Try fetching the full package manifest
    manifest_url = f'{API_BASE_URL}/api/v1/metadata/reference-data/packages/{pkg_name}/{pkg_version}'
    
    try:
        manifest_resp = requests.get(manifest_url, headers=headers, verify=False, timeout=60)
        manifest_resp.raise_for_status()
        manifest_data = manifest_resp.json()
        
        print(f"\nPackage Entries ({len(manifest_data.get('entries', []))}):")
        
        # Look for Incident-related entries
        entries = manifest_data.get('entries', [])
        incident_entries = [e for e in entries if 'incident' in e.get('itemName', '').lower()]
        
        print(f"\nIncident-related entries found: {len(incident_entries)}\n")
        for entry in entries:  # Show first 15
            print(f"  Item: {entry.get('itemName')}")
            print(f"    Version: {entry.get('version')}")
            print(f"    Schema Version: {entry.get('schemaVersion')}")
            print(f"    List Name: {entry.get('listName')}")
            print()
            
    except Exception as e:
        print(f"Error fetching package manifest: {e}")
        print(f"Available packages: {[p.get('name') + '@' + p.get('version') for p in packages]}")
else:
    print("No packages found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fetch Controlled Lists for Field Mapping

# CELL ********************

# List available controlled lists
lists_url = f'{API_BASE_URL}/api/v1/metadata/reference-data/controlled-lists'
lists_resp = requests.get(lists_url, headers=headers, verify=False, timeout=60, params={'size': 100})
lists_resp.raise_for_status()
lists_data = lists_resp.json()

controlled_lists = lists_data.get('content', [])
print(f'Found {len(controlled_lists)} Controlled Lists:\n')

# Create a summary dataframe
list_summary = []
for lst in controlled_lists:
    list_summary.append({
        'Name': lst.get('listName'),
        'Item Name': lst.get('itemName'),
        'Version': lst.get('version'),
        'Item Count': len(lst.get('items', []))
    })

df_lists = pd.DataFrame(list_summary)
display(df_lists)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fetch a Specific Controlled List (Example: IncidentType)

# CELL ********************

# Example: Fetch IncidentType controlled list to see display name mappings
# First, find the correct name from available lists

# Get latest version of a controlled list
list_name = "IncidentStatusType"  # Change this to the field name you're interested in
controlled_list_url = f'{API_BASE_URL}/api/v1/metadata/reference-data/controlled-lists/{list_name}/latest'

try:
    cl_resp = requests.get(controlled_list_url, headers=headers, verify=False, timeout=60)
    cl_resp.raise_for_status()
    controlled_list = cl_resp.json()
    
    print(f"Controlled List: {controlled_list.get('listName')}")
    print(f"Version: {controlled_list.get('version')}")
    print(f"Version Date: {controlled_list.get('versionDate')}")
    print(f"\nItems ({len(controlled_list.get('items', []))}):\n")
    
    items = controlled_list.get('items', [])
    for item in items[:10]:  # Show first 10
        print(f"  ID: {item.get('id')}")
        print(f"    Name (Display): {item.get('name')}")
        print(f"    External ID: {item.get('externalId')}")
        if item.get('guidance'):
            print(f"    Guidance: {item.get('guidance')}")
        if item.get('attributes'):
            print(f"    Attributes: {item.get('attributes')}")
        print()
        
except requests.exceptions.HTTPError as e:
    print(f"Error fetching {list_name}: {e.response.status_code}")
    print(f"Check the list name against available controlled lists above")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Field Mapping Dictionary

# CELL ********************

# Build a mapping of technical field names to display names
# This would be done for all controlled lists relevant to your schema

field_mappings = {}

# Fetch multiple controlled lists and build mapping
lists_to_map = ["IncidentStatusType"]  # Add more as needed

for list_name in lists_to_map:
    try:
        url = f'{API_BASE_URL}/api/v1/metadata/reference-data/controlled-lists/{list_name}/latest'
        resp = requests.get(url, headers=headers, verify=False, timeout=60)
        resp.raise_for_status()
        cl = resp.json()
        
        # Build mapping: id -> name
        field_mappings[list_name] = {}
        for item in cl.get('items', []):
            field_mappings[list_name][item.get('id')] = {
                'display_name': item.get('name'),
                'external_id': item.get('externalId'),
                'guidance': item.get('guidance')
            }
            
        print(f"Loaded mapping for {list_name}: {len(field_mappings[list_name])} items")
    except Exception as e:
        print(f"Could not load {list_name}: {e}")

print(f"\nTotal mappings loaded: {len(field_mappings)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Use Mapping to Transform Incident Data

# CELL ********************

# Example: Transform incident data using the mappings
# Assuming you have incident data from a document fetch

def enrich_incident_with_display_names(incident_content, mappings):
    """
    Transform incident data by replacing coded values with display names
    """
    enriched = incident_content.copy()
    
    # Example: Map IncidentType field
    if 'IncidentType' in incident_content and 'IncidentType' in mappings:
        incident_type_id = incident_content.get('IncidentType')
        if incident_type_id in mappings['IncidentType']:
            enriched['IncidentType_Display'] = mappings['IncidentType'][incident_type_id]['display_name']
    
    return enriched

# Example usage (uncomment with real data):
# sample_incident = {'IncidentType': '1', 'IncidentNumber': '12345'}
# enriched = enrich_incident_with_display_names(sample_incident, field_mappings)
# print(enriched)

print("Enrichment function created. Use enrich_incident_with_display_names() to transform incident data.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Search Documents and Enrich Results

# CELL ********************

# Example: Search for documents and enrich with display names

search_url = f'{API_BASE_URL}/api/v1/document/search'
search_payload = {
    'query': {
        'list': {'documentTypes': ['Incident']},
        'match': {'territoryFrsId': '17'}  # Change to your FRS ID
    },
    'cursor': {'size': 100}
}

try:
    search_resp = requests.post(search_url, json=search_payload, headers=headers, verify=False, timeout=60)
    search_resp.raise_for_status()
    search_results = search_resp.json()
    
    results = search_results.get('results', [])
    print(f"Found {len(results)} incidents")
    
    # Show first incident with enriched data
    if results:
        first_incident = results[0]
        print(f"\nFirst Incident:")
        print(json.dumps(first_incident.get('content'), indent=2))
        
except Exception as e:
    print(f"Search error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
