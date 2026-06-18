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

# # 01 Bronze Full Load (Dual-Search)
# 
# One-time (or occasional) full extract of all FaRDaP documents into Lakehouse Delta tables.
# 
# **Dual-Search Strategy:**
# - Searches **territoryFrsId** - incidents that occurred in our territory
# - Searches **responsibleFrsId** - incidents we were responsible for (even if elsewhere)
# - Deduplicates IDs before fetching (99.6% overlap = significant efficiency gain)
# - Captures ~250-700 cross-border incidents missed by territory-only search
# 
# **When to run:**
# - Initial setup (once)
# - Disaster recovery (full refresh)
# - Data validation/reconciliation
# 
# **Output tables:**
# - `fardap_bronze_incidents` - All incidents in raw JSON form
# - `fardap_bronze_cdc_log` - Change tracking log (all records marked as 'insert' on first run)
# - `fardap_sync_state` - Dual watermarks (last_watermark_territory, last_watermark_responsible)
# 
# **Duration:** 30 mins - several hours (depending on volume)

# MARKDOWN ********************

# ## Configuration
# 
# Store secrets in Fabric Key Vault; do not hard-code in source control.
# - `API_BASE_URL` should be the FaRDaP service endpoint
# - `FRS_ID` is your territory identifier
# - `BATCH_SIZE` and `MAX_WORKERS` control throughput vs rate limits

# CELL ********************

# Get the Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

# Configuration from Fabric variables and Key Vault
API_BASE_URL = vl.getVariable("API_BASE_URL")
KEY_VAULT_URI = vl.getVariable("KEY_VAULT_URI")
USERNAME = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-USERNAME")
PASSWORD = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-PASSWORD")
FRS_ID = vl.getVariable("FRS_ID")

LAKEHOUSE_NAME = vl.getVariable("LAKEHOUSE_NAME")
TABLE_FULL = 'fardap_bronze_incidents'
TABLE_CDC = 'fardap_bronze_cdc_log'
TABLE_STATE = 'fardap_sync_state'

BATCH_SIZE = 1000
MAX_WORKERS = 32
MAX_ATTEMPTS = 5
BASE_BACKOFF = 0.5
REFRESH_EVERY = 25000

print(f'🔧 Configuration:')
print(f'   API: {API_BASE_URL}')
print(f'   FRS: {FRS_ID}')
print(f'   Full Table: {TABLE_FULL}')
print(f'   CDC Table: {TABLE_CDC}')
print(f'   State Table: {TABLE_STATE}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import time
import random
import threading
import pandas as pd
from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F

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
        expires_in = tokens.get('expiresIn', 600)  # Spec: tokens ~20 min; conservative default
        
        if not new_token:
            raise RuntimeError('No access token in auth response')
        
        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        
        with token_lock:
            shared_token = new_token
            shared_refresh_token = new_refresh
            token_expiry = expiry_time
        
        print(f'✅ Authenticated successfully (expires at {expiry_time.strftime("%H:%M:%S UTC")})')
        return new_token
    except Exception as e:
        print(f'❌ Authentication failed: {e}')
        raise

def refresh_access_token():
    """Refresh access token via /auth/access-token-refresh; falls back to full re-auth on failure."""
    global shared_token, shared_refresh_token, token_expiry
    with token_lock:
        refresh_snapshot = shared_refresh_token
    if not refresh_snapshot:
        return authenticate()
    try:
        resp = requests.post(
            f'{API_BASE_URL}/api/v1/auth/access-token-refresh',
            json={'username': USERNAME, 'refreshToken': refresh_snapshot},
            timeout=30
        )
        resp.raise_for_status()
        tokens = resp.json().get('tokens', {})
        new_token = tokens.get('accessToken')
        new_refresh = tokens.get('refreshToken') or refresh_snapshot
        expires_in = tokens.get('expiresIn', 600)
        
        if not new_token:
            raise RuntimeError('No access token in refresh response')
        
        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        
        with token_lock:
            shared_token = new_token
            shared_refresh_token = new_refresh
            token_expiry = expiry_time
        
        print(f'🔄 Access token refreshed (expires at {expiry_time.strftime("%H:%M:%S UTC")})')
        return new_token
    except Exception as e:
        print(f'⚠️  Token refresh failed ({e}); falling back to full re-auth')
        return authenticate()

def is_token_expiring(buffer_seconds=120):
    """Check if token will expire within buffer_seconds (default 2 minutes for ~20-min tokens)."""
    with token_lock:
        if token_expiry is None:
            return True
        time_remaining = (token_expiry - datetime.now(timezone.utc)).total_seconds()
        return time_remaining < buffer_seconds

def make_session():
    # Proactive token refresh if expiring soon
    if is_token_expiring():
        refresh_access_token()
    
    with token_lock:
        token_snapshot = shared_token
    s = requests.Session()
    s.headers.update({
        'Authorization': f'Bearer {token_snapshot}',
        'Content-Type': 'application/json',
        'User-Agent': f'Fabric/FaRDaP-Analytical-Platform/FRS-{FRS_ID}'
    })
    return s

authenticate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch incident IDs using search endpoint with configurable match criteria
def search_by_criteria(match_field, frs_id, batch_size=BATCH_SIZE):
    """
    Search for incidents by a specific match field (territoryFrsId or responsibleFrsId).
    
    Args:
        match_field: Either 'territoryFrsId' or 'responsibleFrsId'
        frs_id: The FRS ID to search for
        batch_size: Number of results per page
    
    Returns:
        Tuple of (list of document IDs, list of search result objects with dateUpdated)
    """
    url = f'{API_BASE_URL}/api/v1/document/search'
    cursor = None
    ids = []
    search_results = []
    page = 0
    
    print(f'🔍 Searching for {match_field}={frs_id}...')
    
    while True:
        page += 1
        payload = {
            'query': {
                'list': {'documentTypes': ['Incident']},
                'match': {match_field: str(frs_id)}
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
        
        # Extract IDs and keep full results for watermark tracking
        for doc in results:
            doc_id = doc.get('properties', {}).get('documentId')
            if doc_id:
                ids.append(doc_id)
                search_results.append(doc)
        
        new_cursor = data.get('search', {}).get('cursor', {}).get('lastDocumentValues')
        print(f'   📄 Page {page}: +{len([d for d in results if d.get("properties", {}).get("documentId")]):,} IDs | Total: {len(ids):,} | Has more: {bool(new_cursor)}')
        
        if not new_cursor or len(results) == 0:
            break
        cursor = new_cursor
    
    print(f'   ✅ {match_field} search complete: {page} pages, {len(ids):,} total IDs')
    return ids, search_results

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

print('✅ Fetch functions defined')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute dual-search: territoryFrsId + responsibleFrsId
import concurrent.futures

print('='*60)
print('🔍 DUAL-SEARCH: Territory + Responsible')
print('='*60)

# Search 1: territoryFrsId (incidents that occurred in our territory)
territory_ids, territory_results = search_by_criteria('territoryFrsId', FRS_ID)

# Search 2: responsibleFrsId (incidents we were responsible for, even if elsewhere)
responsible_ids, responsible_results = search_by_criteria('responsibleFrsId', FRS_ID)

# Calculate overlap and deduplicate
territory_set = set(territory_ids)
responsible_set = set(responsible_ids)
all_unique_ids = list(territory_set | responsible_set)

overlap_count = len(territory_set & responsible_set)
territory_only = len(territory_set - responsible_set)
responsible_only = len(responsible_set - territory_set)

print('\n' + '='*60)
print('📊 DUAL-SEARCH SUMMARY')
print('='*60)
print(f'Territory IDs:     {len(territory_ids):>8,}')
print(f'Responsible IDs:   {len(responsible_ids):>8,}')
print(f'---')
print(f'Overlap:           {overlap_count:>8,} ({overlap_count/max(len(all_unique_ids),1)*100:.1f}%)')
print(f'Territory only:    {territory_only:>8,}')
print(f'Responsible only:  {responsible_only:>8,}')
print(f'---')
print(f'Total unique:      {len(all_unique_ids):>8,}')
print('='*60)

results = []
completed = 0

print(f'\n📥 Fetching {len(all_unique_ids):,} incidents in parallel (MAX_WORKERS={MAX_WORKERS})...')

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = [pool.submit(fetch_one, doc_id) for doc_id in all_unique_ids]
    for fut in concurrent.futures.as_completed(futures):
        rec = fut.result()
        if rec:
            results.append(rec)
        completed += 1
        if completed % 500 == 0:
            print(f'   Progress: {completed:,}/{len(all_unique_ids):,} ({len(results):,} with data)')
        if completed % REFRESH_EVERY == 0:
            print(f'   🔄 Periodic token refresh...')
            refresh_access_token()

print(f'\n✅ Fetch complete: {len(results):,} / {len(all_unique_ids):,} with data')
assert len(results) > 0, '❌ No documents retrieved; aborting write'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build DataFrame and prepare for write
from pyspark.sql.types import StructType, StructField, StringType

df_raw = spark.createDataFrame(pd.DataFrame(results))
df_raw = df_raw.withColumn('sync_timestamp', F.col('sync_timestamp').cast('timestamp'))
df_raw = df_raw.withColumn('change_ts', F.col('change_ts').cast('timestamp'))

# Add content hash for change detection
df_raw = df_raw.withColumn('content_hash', F.sha2(F.col('raw_json'), 256))

print(f'📊 Created DataFrame with {df_raw.count():,} rows')
print(f'   Columns: {df_raw.columns}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to Bronze table (full load mode = OVERWRITE)
print(f'\n💾 Writing to {TABLE_FULL}...')

df_raw.write \
    .format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .saveAsTable(TABLE_FULL)

print(f'✅ Wrote {df_raw.count():,} records to {TABLE_FULL}')

# Create CDC log (all records marked as 'insert' for full load)
df_cdc = df_raw.select(
    F.col('documentId'),
    F.lit('insert').alias('op_type'),
    F.col('change_ts'),
    F.col('sync_timestamp')
)

print(f'\n📋 Writing CDC log ({TABLE_CDC})...')
df_cdc.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(TABLE_CDC)

print(f'✅ Wrote {df_cdc.count():,} records to {TABLE_CDC}')

# Create/update state table with dual watermarks
watermark = df_raw.select(F.coalesce(F.col('change_ts'), F.col('sync_timestamp')).alias('ts')) \
    .agg(F.max('ts')).collect()[0][0]

# Standardise as ISO-8601 STRING for stable schema across full/incremental paths
if watermark is None:
    watermark_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
elif hasattr(watermark, 'strftime'):
    watermark_str = watermark.strftime("%Y-%m-%dT%H:%M:%SZ")
else:
    watermark_str = str(watermark)

# Create state table with dual watermarks
# Initialize both territory and responsible watermarks to the same value
df_state = spark.createDataFrame([
    (watermark_str, watermark_str, watermark_str)
], ['last_watermark', 'last_watermark_territory', 'last_watermark_responsible'])

print(f'\n🏁 Initializing dual watermarks:')
print(f'   Territory watermark:    {watermark_str}')
print(f'   Responsible watermark:  {watermark_str}')
print(f'   (last_watermark kept for backwards compatibility)')

df_state.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(TABLE_STATE)

print(f'✅ State table updated with dual watermarks')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final verification
print('\n' + '='*60)
print('📊 FINAL VERIFICATION')
print('='*60)

df_full = spark.table(TABLE_FULL)
print(f'\n✅ {TABLE_FULL}:')
print(f'   Rows: {df_full.count():,}')
print(f'   Columns: {len(df_full.columns)}')
print(f'   Schema: {df_full.schema.simpleString()}')

df_cdc_check = spark.table(TABLE_CDC)
print(f'\n✅ {TABLE_CDC}:')
print(f'   Rows: {df_cdc_check.count():,}')
print(f'   Op types: {df_cdc_check.groupBy("op_type").count().collect()}')

df_state_check = spark.table(TABLE_STATE)
print(f'\n✅ {TABLE_STATE}:')
df_state_check.show(truncate=False)

print('\n🎉 Full Bronze load completed successfully!')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
