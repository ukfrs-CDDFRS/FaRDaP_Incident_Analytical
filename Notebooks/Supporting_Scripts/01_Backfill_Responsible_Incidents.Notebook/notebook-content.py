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

# # 01 Backfill Responsible Incidents
# 
# **Purpose:** One-time backfill of historical cross-border incidents
# 
# ## Background:
# 
# Before dual-search implementation, Bronze ingestion only fetched incidents where:
# - `territoryFrsId = 17` (incidents that occurred in Humberside)
# 
# This missed ~250-700 incidents where:
# - `responsibleFrsId = 17` (Humberside was responsible)
# - `territoryFrsId ≠ 17` (incident occurred elsewhere)
# 
# ## What This Does:
# 
# 1. **Search all** `responsibleFrsId=17` incidents from FaRDaP API
# 2. **Anti-join** against existing Bronze to identify missing incidents
# 3. **Fetch only missing** incidents (efficient, no duplicate API calls)
# 4. **Append** to Bronze with `op_type='backfill_insert'` in CDC log
# 
# ## When to Run:
# - **ONCE** after deploying dual-search notebooks
# - After full load completes (ensures Bronze is populated)
# - Before enabling incremental sync (to have complete baseline)
# 
# ## Safety:
# - ✅ Read-only search (doesn't modify existing data)
# - ✅ Anti-join ensures no duplicates
# - ✅ Append-only (doesn't overwrite)
# - ✅ DRY_RUN mode available
# 
# ## Expected Results:
# - ~250-700 new incidents appended to Bronze
# - All historical cross-border incidents captured
# - Complete baseline for incremental sync

# MARKDOWN ********************

# ## Configuration

# CELL ********************

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from datetime import datetime, timezone, timedelta
import requests
import json
import time
import random
import threading
import pandas as pd

# Get Spark session
spark = SparkSession.builder.getOrCreate()

# Get Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

# Configuration
API_BASE_URL = vl.getVariable("API_BASE_URL")
FRS_ID = int(vl.getVariable("FRS_ID"))
USERNAME = vl.getVariable("USERNAME")
PASSWORD = vl.getVariable("PASSWORD")
BATCH_SIZE = int(vl.getVariable("BATCH_SIZE", "5000"))
MAX_WORKERS = int(vl.getVariable("MAX_WORKERS", "10"))
LAKEHOUSE_NAME = vl.getVariable("LAKEHOUSE_NAME")

# Table names
TABLE_BRONZE = 'fardap_bronze_incidents'
TABLE_CDC = 'fardap_bronze_cdc_log'

# Backoff settings
MAX_ATTEMPTS = 3
BASE_BACKOFF = 1.0
REFRESH_EVERY = 2000

# DRY RUN MODE: Set to True to preview without writing
DRY_RUN = False

print(f"🔧 Configuration:")
print(f"   API Base URL: {API_BASE_URL}")
print(f"   FRS ID: {FRS_ID}")
print(f"   Batch Size: {BATCH_SIZE}")
print(f"   Max Workers: {MAX_WORKERS}")
print(f"   DRY RUN: {DRY_RUN}")

# MARKDOWN ********************

# ## Authentication

# CELL ********************

# Authentication and session helpers
token_lock = threading.Lock()
shared_token = None
shared_refresh_token = None
token_expiry = None

def authenticate():
    """Full authentication via /auth/init using username/password."""
    global shared_token, shared_refresh_token, token_expiry
    try:
        resp = requests.post(
            f'{API_BASE_URL}/api/v1/auth/init',
            json={'username': USERNAME, 'password': PASSWORD},
            timeout=30
        )
        resp.raise_for_status()
        tokens = resp.json().get('tokens', {})
        new_token = tokens.get('accessToken')
        new_refresh = tokens.get('refreshToken')
        expires_in = tokens.get('expiresIn', 600)
        
        if not new_token:
            raise RuntimeError('No access token in auth response')
        
        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        
        with token_lock:
            shared_token = new_token
            shared_refresh_token = new_refresh
            token_expiry = expiry_time
        
        print(f'✅ Authenticated successfully (expires: {expiry_time.isoformat()})')
    except Exception as e:
        print(f'❌ Authentication failed: {e}')
        raise

def refresh_access_token():
    """Refresh access token using refresh token."""
    global shared_token, shared_refresh_token, token_expiry
    
    with token_lock:
        current_refresh = shared_refresh_token
    
    if not current_refresh:
        print('⚠️  No refresh token available, re-authenticating...')
        authenticate()
        return
    
    try:
        resp = requests.post(
            f'{API_BASE_URL}/api/v1/auth/refresh',
            json={'refreshToken': current_refresh},
            timeout=30
        )
        resp.raise_for_status()
        tokens = resp.json().get('tokens', {})
        new_token = tokens.get('accessToken')
        new_refresh = tokens.get('refreshToken')
        expires_in = tokens.get('expiresIn', 600)
        
        if not new_token:
            print('⚠️  Token refresh failed, re-authenticating...')
            authenticate()
            return
        
        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        
        with token_lock:
            shared_token = new_token
            shared_refresh_token = new_refresh
            token_expiry = expiry_time
        
    except Exception as e:
        print(f'⚠️  Token refresh error: {e}, re-authenticating...')
        authenticate()

def is_token_expiring():
    """Check if token expires within 2 minutes."""
    with token_lock:
        exp = token_expiry
    if exp is None:
        return True
    return datetime.now(timezone.utc) >= exp - timedelta(minutes=2)

def make_session():
    """Create requests session with current token."""
    if is_token_expiring():
        refresh_access_token()
    
    with token_lock:
        token_snapshot = shared_token
    s = requests.Session()
    s.headers.update({
        'Authorization': f'Bearer {token_snapshot}',
        'Content-Type': 'application/json',
        'User-Agent': f'Fabric/FaRDaP-Backfill/FRS-{FRS_ID}'
    })
    return s

authenticate()

# MARKDOWN ********************

# ## Step 1: Search All Responsible Incidents

# CELL ********************

# Search for all responsibleFrsId incidents
def search_all_responsible(batch_size=BATCH_SIZE):
    """Search for all incidents where responsibleFrsId = FRS_ID."""
    url = f'{API_BASE_URL}/api/v1/document/search'
    cursor = None
    ids = []
    page = 0
    
    print(f'🔍 Searching all responsibleFrsId={FRS_ID} incidents...')
    
    while True:
        page += 1
        payload = {
            'query': {
                'list': {'documentTypes': ['Incident']},
                'match': {'responsibleFrsId': str(FRS_ID)}
            },
            'cursor': {'size': batch_size}
        }
        if cursor:
            payload['cursor']['lastDocumentValues'] = cursor
        
        s = make_session()
        resp = s.post(url, json=payload, timeout=60)
        
        if resp.status_code == 401:
            authenticate()
            s = make_session()
            resp = s.post(url, json=payload, timeout=60)
        
        resp.raise_for_status()
        data = resp.json()
        errors = data.get('errors') or []
        if errors:
            raise RuntimeError(f'FaRDaP search returned errors: {errors}')
        results = data.get('results', [])
        
        new_ids = [
            d.get('properties', {}).get('documentId')
            for d in results
            if d.get('properties', {}).get('documentId')
        ]
        ids.extend(new_ids)
        
        new_cursor = data.get('search', {}).get('cursor', {}).get('lastDocumentValues')
        print(f'   📄 Page {page}: +{len(new_ids):,} IDs | Total: {len(ids):,} | Has more: {bool(new_cursor)}')
        
        if not new_cursor or len(results) == 0:
            break
        cursor = new_cursor
    
    print(f'   ✅ Search complete: {len(ids):,} total responsibleFrsId incidents')
    return ids

# Execute search
all_responsible_ids = search_all_responsible()

# MARKDOWN ********************

# ## Step 2: Identify Missing Incidents (Anti-Join)

# CELL ********************

# Read existing Bronze incidents
try:
    df_bronze = spark.table(TABLE_BRONZE)
    existing_ids = set([row['documentId'] for row in df_bronze.select('documentId').distinct().collect()])
    print(f'📊 Existing Bronze incidents: {len(existing_ids):,}')
except Exception as e:
    print(f'⚠️  Could not read Bronze table: {e}')
    print('   Assuming empty table (first run)')
    existing_ids = set()

# Anti-join: find IDs in responsible search but NOT in Bronze
responsible_ids_set = set(all_responsible_ids)
missing_ids = list(responsible_ids_set - existing_ids)

print(f'\n' + '='*60)
print('📊 ANTI-JOIN ANALYSIS')
print('='*60)
print(f'Total responsibleFrsId IDs:  {len(responsible_ids_set):>8,}')
print(f'Already in Bronze:           {len(responsible_ids_set & existing_ids):>8,}')
print(f'---')
print(f'Missing (to backfill):       {len(missing_ids):>8,}')
print('='*60)

if len(missing_ids) == 0:
    print('\n✅ No missing incidents found. Backfill not needed.')
    print('   All responsibleFrsId incidents are already in Bronze.')
    notebookutils.notebook.exit("no_backfill_needed")

print(f'\n📥 Will fetch {len(missing_ids):,} missing incidents')

# MARKDOWN ********************

# ## Step 3: Fetch Missing Incidents

# CELL ********************

# Fetch individual incident details
def fetch_one(doc_id):
    attempts = 0
    while attempts < MAX_ATTEMPTS:
        attempts += 1
        try:
            s = make_session()
            resp = s.get(
                f'{API_BASE_URL}/api/v1/document/{doc_id}',
                params={'frsId': FRS_ID},
                timeout=30
            )
            
            if resp.status_code == 401 and attempts == 1:
                authenticate()
                continue
            
            if resp.status_code == 429:
                retry_after = resp.headers.get('Retry-After')
                delay = float(retry_after) if retry_after else BASE_BACKOFF * (2 ** (attempts - 1)) + random.uniform(0, 0.5)
                time.sleep(delay)
                continue
            
            if resp.status_code >= 500:
                delay = BASE_BACKOFF * (2 ** (attempts - 1)) + random.uniform(0, 0.5)
                time.sleep(delay)
                continue
            
            resp.raise_for_status()
            body = resp.json()
            props = body.get('properties') or {}
            audit = (body.get('content') or {}).get('auditDetail') or {}
            change_ts = props.get('dateUpdated') or audit.get('dateUpdated')
            
            return {
                'documentId': doc_id,
                'raw_json': json.dumps(body, ensure_ascii=False),
                'sync_timestamp': datetime.now(timezone.utc).isoformat(),
                'change_ts': change_ts
            }
        except Exception as e:
            if attempts == MAX_ATTEMPTS:
                print(f'⚠️  Failed to fetch {doc_id} after {MAX_ATTEMPTS} attempts: {e}')
            delay = BASE_BACKOFF * (2 ** (attempts - 1)) + random.uniform(0, 0.5)
            time.sleep(delay)
    
    return None

# Fetch missing incidents in parallel
results = []
completed = 0

print(f'\n📥 Fetching {len(missing_ids):,} missing incidents in parallel (MAX_WORKERS={MAX_WORKERS})...')

import concurrent.futures

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = [pool.submit(fetch_one, doc_id) for doc_id in missing_ids]
    for fut in concurrent.futures.as_completed(futures):
        rec = fut.result()
        if rec:
            results.append(rec)
        completed += 1
        if completed % 100 == 0:
            print(f'   Progress: {completed:,}/{len(missing_ids):,} ({len(results):,} with data)')
        if completed % REFRESH_EVERY == 0:
            print(f'   🔄 Periodic token refresh...')
            refresh_access_token()

print(f'\n✅ Fetch complete: {len(results):,} / {len(missing_ids):,} documents fetched')

if len(results) == 0:
    print('\n⚠️  No documents fetched. Exiting.')
    notebookutils.notebook.exit("no_data_fetched")

# MARKDOWN ********************

# ## Step 4: Prepare DataFrame

# CELL ********************

# Build DataFrame
df_backfill = spark.createDataFrame(pd.DataFrame(results))
df_backfill = df_backfill.withColumn('sync_timestamp', F.col('sync_timestamp').cast('timestamp'))
df_backfill = df_backfill.withColumn('change_ts', F.col('change_ts').cast('timestamp'))
df_backfill = df_backfill.withColumn('content_hash', F.sha2(F.col('raw_json'), 256))

print(f'📊 Created backfill DataFrame with {df_backfill.count():,} rows')
print(f'   Columns: {df_backfill.columns}')

# Preview sample records
print(f'\n📄 Sample backfill records:')
df_backfill.select('documentId', 'change_ts', 'sync_timestamp').show(5, truncate=False)

# MARKDOWN ********************

# ## Step 5: Write to Bronze (Append)

# CELL ********************

if DRY_RUN:
    print('🔍 DRY RUN MODE: Would have written to Bronze, but skipping.')
    print(f'   Would append {df_backfill.count():,} rows to {TABLE_BRONZE}')
    print(f'   Would append {df_backfill.count():,} rows to {TABLE_CDC}')
else:
    # Append to Bronze table
    print(f'\n💾 Appending {df_backfill.count():,} backfilled incidents to {TABLE_BRONZE}...')
    
    df_backfill.write \
        .format('delta') \
        .mode('append') \
        .option('mergeSchema', 'true') \
        .saveAsTable(TABLE_BRONZE)
    
    print(f'✅ Appended to {TABLE_BRONZE}')
    
    # Create CDC log entries (op_type = 'backfill_insert')
    df_cdc_backfill = df_backfill.select(
        F.col('documentId'),
        F.lit('backfill_insert').alias('op_type'),
        F.col('change_ts'),
        F.col('sync_timestamp')
    )
    
    print(f'\n📋 Appending backfill CDC log ({TABLE_CDC})...')
    df_cdc_backfill.write \
        .format('delta') \
        .mode('append') \
        .saveAsTable(TABLE_CDC)
    
    print(f'✅ Appended to {TABLE_CDC}')

# MARKDOWN ********************

# ## Step 6: Verification

# CELL ********************

if not DRY_RUN:
    print('\n' + '='*60)
    print('📊 BACKFILL VERIFICATION')
    print('='*60)
    
    # Re-read Bronze to confirm
    df_bronze_after = spark.table(TABLE_BRONZE)
    print(f'\n✅ Bronze table ({TABLE_BRONZE}):')
    print(f'   Total rows: {df_bronze_after.count():,}')
    
    # Check CDC log
    df_cdc_check = spark.table(TABLE_CDC)
    backfill_count = df_cdc_check.filter(F.col('op_type') == 'backfill_insert').count()
    print(f'\n✅ CDC log ({TABLE_CDC}):')
    print(f'   Backfill inserts: {backfill_count:,}')
    print(f'   Op type breakdown:')
    df_cdc_check.groupBy('op_type').count().orderBy(F.col('count').desc()).show(10, truncate=False)
    
    # Verify missing IDs now exist
    bronze_ids_after = set([row['documentId'] for row in df_bronze_after.select('documentId').distinct().collect()])
    still_missing = set(missing_ids) - bronze_ids_after
    
    if len(still_missing) > 0:
        print(f'\n⚠️  WARNING: {len(still_missing)} IDs still missing after backfill!')
        print(f'   This might indicate fetch failures. Review logs above.')
    else:
        print(f'\n✅ All {len(missing_ids):,} missing incidents successfully backfilled!')
    
    print('='*60)

print('\n🎉 Backfill completed successfully!')

# MARKDOWN ********************

# ## Summary
# 
# ### What Happened:
# 
# 1. ✅ Searched all `responsibleFrsId=17` incidents from API
# 2. ✅ Anti-joined against existing Bronze to find missing incidents
# 3. ✅ Fetched only missing incidents (~250-700 expected)
# 4. ✅ Appended to Bronze with `op_type='backfill_insert'`
# 5. ✅ Verified all missing incidents now in Bronze
# 
# ### Next Steps:
# 
# 1. **Review verification output** above to confirm backfill success
# 2. **Run incremental sync** to test dual-watermark logic
# 3. **Monitor CDC log** to ensure backfilled incidents appear correctly
# 4. **Compare counts** with expected cross-border incident volume
# 
# ### Rollback (if needed):
# 
# If you need to rollback the backfill:
# 
# ```python
# # Remove backfilled incidents from Bronze
# backfilled_ids = [row['documentId'] for row in spark.table(TABLE_CDC).filter(F.col('op_type') == 'backfill_insert').select('documentId').collect()]
# df_bronze = spark.table(TABLE_BRONZE)
# df_bronze.filter(~F.col('documentId').isin(backfilled_ids)).write.format('delta').mode('overwrite').saveAsTable(TABLE_BRONZE)
# 
# # Remove backfill CDC entries
# df_cdc = spark.table(TABLE_CDC)
# df_cdc.filter(F.col('op_type') != 'backfill_insert').write.format('delta').mode('overwrite').saveAsTable(TABLE_CDC)
# ```

# MARKDOWN ********************

# ---
# 
# **Notebook:** 01_Backfill_Responsible_Incidents  
# **Purpose:** One-time historical backfill of cross-border incidents  
# **Version:** 1.0  
# **Created:** 2026-06-18  
# **Phase:** Dual-Search Bronze Ingestion (Phase 3)
