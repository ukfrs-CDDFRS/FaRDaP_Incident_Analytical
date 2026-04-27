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

# # 01 Bronze Incremental Sync
# 
# Fetch only **changed/new documents** from FaRDaP API since the last successful run.
# 
# **When to run:**
# - Every 5-10 minutes (production schedule)
# - After full load completes
# - Continuously for near-real-time updates
# 
# **Input:**
# - Last watermark from `fardap_sync_state` (highest `change_ts` processed)
# 
# **Output tables:**
# - `fardap_bronze_incidents` - Updated with new/changed records (MERGE mode)
# - `fardap_bronze_cdc_log` - Appended with change tracking
# - `fardap_sync_state` - Updated watermark
# 
# **Duration:** 1-2 minutes (network bound)
# 
# **Key benefits:**
# - ✅ Only fetches changed documents
# - ✅ Idempotent (safe to re-run)
# - ✅ Preserves change history in CDC log

# MARKDOWN ********************

# ## Configuration
# 
# Store secrets in Fabric Key Vault; do not hard-code in source control.

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
print(f'   Mode: INCREMENTAL')

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

# Get last watermark
DEFAULT_LOOKBACK_MINUTES = 5

try:
    watermark_df = spark.table(TABLE_STATE)

    rows = (
        watermark_df
        .select("last_watermark")
        .orderBy(F.col("last_watermark").desc())
        .limit(1)
        .collect()
    )

    if not rows or rows[0][0] is None:
        UPDATED_FROM_DT = datetime.now(timezone.utc) - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
        print("📍 No watermark found, using 5-minute lookback")
    else:
        value = rows[0][0]

        # normalise to timezone-aware datetime
        if isinstance(value, str):
            iso = value.strip()
            if iso.endswith("Z"):
                iso = iso[:-1] + "+00:00"
            UPDATED_FROM_DT = datetime.fromisoformat(iso)
            if UPDATED_FROM_DT.tzinfo is None:
                UPDATED_FROM_DT = UPDATED_FROM_DT.replace(tzinfo=timezone.utc)
        else:
            UPDATED_FROM_DT = value if value.tzinfo else value.replace(tzinfo=timezone.utc)

        print(f"📍 Using stored watermark: {UPDATED_FROM_DT}")

except Exception as e:
    print(f"⚠️  Could not read state table: {e}")
    UPDATED_FROM_DT = datetime.now(timezone.utc) - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
    print("📍 Using 5-minute lookback")

UPDATED_FROM = UPDATED_FROM_DT.strftime("%Y-%m-%dT%H:%M:%S.000Z")

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
print('✅ Session helpers initialized')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Search for changed incident IDs since watermark
def search_changed_ids(batch_size=BATCH_SIZE):
    url = f'{API_BASE_URL}/api/v1/document/search'
    cursor = None
    ids = []
    page = 0
    
    while True:
        page += 1
        payload = {
            'query': {
                'list': {'documentTypes': ['Incident']},
                'match': {'territoryFrsId': str(FRS_ID)},
                'range': [
                    {
                        'from': UPDATED_FROM,
                        'field': 'dateUpdated'
                    }
                ]
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
        print(f'📄 Page {page}: Fetched {len(new_ids):,} changed IDs | Total: {len(ids):,} | Has more: {bool(new_cursor)}')
        
        if not new_cursor or len(results) == 0:
            break
        cursor = new_cursor
    
    print(f'✅ Search complete: {len(ids):,} changed incidents since {UPDATED_FROM}')
    return ids

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

# Search for changed IDs
print('🔍 Searching for changed incidents...')
changed_ids = search_changed_ids()

if len(changed_ids) == 0:
    print('\n✅ No changes since last run. Exiting.')
    notebookutils.notebook.exit("success")

# Fetch changed documents in parallel
results = []
completed = 0

print(f'\n📥 Fetching {len(changed_ids):,} changed incidents in parallel (MAX_WORKERS={MAX_WORKERS})...')

import concurrent.futures

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = [pool.submit(fetch_one, doc_id) for doc_id in changed_ids]
    for fut in concurrent.futures.as_completed(futures):
        rec = fut.result()
        if rec:
            results.append(rec)
        completed += 1
        if completed % 500 == 0:
            print(f'   Progress: {completed:,}/{len(changed_ids):,} ({len(results):,} with data)')
        if completed % REFRESH_EVERY == 0:
            print(f'   🔄 Periodic token refresh...')
            refresh_access_token()

print(f'\n✅ Fetch complete: {len(results):,} / {len(changed_ids):,} documents fetched')
assert len(results) > 0, '❌ No documents retrieved'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build DataFrame
df_raw = spark.createDataFrame(pd.DataFrame(results))
df_raw = df_raw.withColumn('sync_timestamp', F.col('sync_timestamp').cast('timestamp'))
df_raw = df_raw.withColumn('content_hash', F.sha2(F.col('raw_json'), 256))

print(f'📊 Created DataFrame with {df_raw.count():,} rows')
print(f'   Columns: {df_raw.columns}')

# Create temporary view
df_raw.createOrReplaceTempView('staging_incremental')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Determine op_type from current Bronze membership BEFORE the MERGE.
# Existing documentId -> 'update'; new documentId -> 'insert'.
existing_ids = spark.table(TABLE_FULL).select('documentId').distinct()
df_tagged = df_raw.join(existing_ids, on='documentId', how='left_anti') \
    .withColumn('op_type', F.lit('insert')) \
    .select('documentId', 'op_type', 'change_ts', 'sync_timestamp') \
    .unionByName(
        df_raw.join(existing_ids, on='documentId', how='left_semi')
              .withColumn('op_type', F.lit('update'))
              .select('documentId', 'op_type', 'change_ts', 'sync_timestamp')
    )
df_tagged.cache()

# Merge into Bronze table (MERGE mode for idempotency)
print(f'\n🔄 Merging into {TABLE_FULL}...')

spark.sql(f'''
MERGE INTO {TABLE_FULL} t
USING staging_incremental s
ON t.documentId = s.documentId
WHEN MATCHED AND t.content_hash <> s.content_hash THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
''')

print(f'✅ Merge completed')

# Append to CDC log
df_cdc = df_tagged

print(f'\n📋 Appending {df_cdc.count():,} records to {TABLE_CDC}...')
df_cdc.write \
    .format('delta') \
    .mode('append') \
    .option('mergeSchema', 'true') \
    .saveAsTable(TABLE_CDC)

print(f'✅ CDC log appended')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update watermark
watermark = df_raw.select(F.coalesce(F.col('change_ts'), F.col('sync_timestamp')).alias('ts')) \
    .agg(F.max('ts')).collect()[0][0]

# Convert to string format for consistency with state table
if watermark:
    watermark_str = watermark.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(watermark, 'strftime') else str(watermark)
else:
    watermark_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

df_state = spark.createDataFrame([(watermark_str,)], ['last_watermark'])

print(f'\n🏁 Updating state watermark to: {watermark_str}')
df_state.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable(TABLE_STATE)

print(f'✅ State table updated')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final verification
print('\n' + '='*60)
print('📊 INCREMENTAL SYNC VERIFICATION')
print('='*60)

df_full = spark.table(TABLE_FULL)
print(f'\n✅ {TABLE_FULL}:')
print(f'   Total rows: {df_full.count():,}')

df_cdc_check = spark.table(TABLE_CDC)
print(f'\n✅ {TABLE_CDC}:')
print(f'   Total rows: {df_cdc_check.count():,}')
print(f'   This run: +{df_cdc.count():,} records')
cdc_summary = df_cdc_check.groupBy("op_type").count().collect()
for row in cdc_summary:
    print(f'      {row["op_type"]}: {row["count"]:,}')

df_state_check = spark.table(TABLE_STATE)
print(f'\n✅ {TABLE_STATE}:')
df_state_check.show(truncate=False)

print('\n🎉 Incremental Bronze sync completed successfully!')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
