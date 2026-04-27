# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Find Your FRS ID
# 
# ## What is an FRS ID?
# 
# The **FRS ID** is a numeric identifier for your Fire and Rescue Service organisation in the FaRDaP™ system. It comes from the **FRSIdListType Reference Data**.
# 
# **Important:** Do NOT use the IRS two-character codes (HS, GM, WY, etc.) — use the numeric FRS ID instead.
# 
# ### Examples:
# - London South → FRS ID: **39**
# - Greater Manchester → FRS ID: **28**
# - West Yorkshire → FRS ID: **42**
# 
# ## How to Find Your FRS ID
# 
# This notebook will:
# 1. Authenticate with the FaRDaP™ API
# 2. Fetch all available FRS organisations from Reference Data
# 3. Display them so you can find yours
# 4. Let you test your FRS ID

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

# ## Step 1: Enter Your Credentials
# 
# Update these with your FaRDaP™ credentials:

# CELL ********************

# Get the Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

get_test = vl.getVariable("API_BASE_URL")

print(get_test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# FaRDaP™ Credentials - UPDATE THESE
API_BASE_URL = vl.getVariable("API_BASE_URL")
KEY_VAULT_URI = vl.getVariable("KEY_VAULT_URI")
USERNAME = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-USERNAME")
PASSWORD = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-PASSWORD")

print("Configuration loaded:")
print(f"  API: {API_BASE_URL}")
print(f"\n⚠ Remember to update USERNAME and PASSWORD before running!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Authenticate with FaRDaP™ API

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
        expires_in = auth_response['tokens'].get('expiresIn', 'unknown')
        
        print(f"✓ Authentication successful!")
        print(f"  Token expires in: {expires_in} seconds")
        print(f"\nReady to query FRS organisations!")
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

# ## Step 3: Fetch All FRS Organisations from Reference Data
# 
# Query the FRSIdListType Reference Data to get all available FRS organisations:

# CELL ********************

if not access_token:
    print("✗ Cannot proceed - authentication failed. Check your credentials and try again.")
else:
    # Query reference data for FRS organisations
    ref_data_url = f"{API_BASE_URL}/api/v1/metadata/reference-data/controlled-lists/FRSIdListType/latest"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    try:
        print("Fetching FRS organisations from Reference Data...\n")
        
        response = session.get(
            ref_data_url,
            headers=headers,
            verify=False,
            timeout=30
        )
        
        if response.status_code == 200:
            ref_data = response.json()
            items = ref_data.get('items', [])
            
            if items:
                print(f"✓ Found {len(items)} FRS organisations:\n")
                
                # Prepare table data
                table_data = []
                for item in items:
                    frs_id = item.get('id', 'N/A')
                    name = item.get('name', 'N/A')
                    table_data.append([frs_id, name])
                
                # Display as table
                print(tabulate(table_data, headers=['FRS ID', 'FRS Organisation'], tablefmt='grid'))
                
                print(f"\n{'='*60}")
                print("✓ YOUR FRS ID is the numeric value in the left column")
                print(f"{'='*60}")
                
                # Store for later use
                frs_list = {item.get('id'): item.get('name') for item in items}
            else:
                print("⚠ No FRS organisations found in reference data")
                frs_list = {}
        else:
            print(f"✗ Failed to fetch reference data: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            frs_list = {}
            
    except Exception as e:
        print(f"✗ Error fetching reference data: {str(e)}")
        frs_list = {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Test Your FRS ID (Optional)
# 
# Once you've found your FRS ID, test it by running a search with it:
# 
# Enter your FRS ID below and run this cell to verify it works:

# CELL ********************

# Test FRS ID - UPDATE THIS
TEST_FRS_ID = 17  # e.g., "39" for London South

if not access_token:
    print("✗ Not authenticated. Run the authentication cell first.")
elif TEST_FRS_ID == "[REPLACE_WITH_YOUR_FRS_ID]":
    print("⚠ Please update TEST_FRS_ID with your actual FRS ID from Step 3 above")
else:
    try:
        print(f"Testing FRS ID: {TEST_FRS_ID}\n")
        
        # Run a simple search with this FRS ID
        search_url = f"{API_BASE_URL}/api/v1/document/search"
        headers = {'Authorization': f'Bearer {access_token}'}
        
        search_payload = {
            "query": {
                "match": {
                    "territoryFrsId": TEST_FRS_ID
                }
            },
            "cursor": {
                "size": 10
            }
        }
        
        response = session.post(
            search_url,
            headers=headers,
            json=search_payload,
            verify=False,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            document_count = len(result.get('results', []))
            
            print(f"✓ FRS ID {TEST_FRS_ID} is VALID!")
            print(f"  Organisation: {frs_list.get(TEST_FRS_ID, 'Unknown')}")
            print(f"  Documents found: {document_count}")
            print(f"\n✓ Ready to use this FRS ID in the bulk load notebooks!")
        else:
            print(f"✗ Search failed: HTTP {response.status_code}")
            print(f"  This FRS ID may not be valid for your account")
            print(f"  Response: {response.text}")
            
    except Exception as e:
        print(f"✗ Error testing FRS ID: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# 
# You now have your **FRS ID**!
# 
# ### Next Steps:
# 1. **Copy your FRS ID** from the table in Step 3
# 2. **Use it in the bulk load notebooks:**
#    - Update `[YOUR_FRS_ID]` in `FaRDaP_Bulk_Load.ipynb`
#    - Update `[YOUR_FRS_ID]` in `FaRDaP_Incremental_Sync.ipynb`
# 3. **Run the notebooks** to start syncing data
# 
# ### Important Reminders:
# - ✓ Use the **numeric FRS ID** (e.g., `39` not `HS`)
# - ✓ Put FRS ID in **quotes** as a string (e.g., `"39"` not `39`)
# - ✓ Don't confuse with IRS two-character codes
# 
# ### Still Can't Find It?
# Contact: **fardaphelp@communities.gov.uk**
