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

# # 01 Bronze Incremental Sync (Dual-Search)
# 
# Fetch only **changed/new documents** from FaRDaP API since the last successful run.
# 
# **Dual-Search Strategy:**
# - Searches **territoryFrsId** with territory watermark - incidents in our territory
# - Searches **responsibleFrsId** with responsible watermark - incidents we're responsible for
# - Independent watermarks prevent missing updates when searches have different highest timestamps
# - Deduplicates IDs before fetching (significant efficiency gain)
# - Updates both watermarks separately from each search's results
# 
# **When to run:**
# - Every 5-10 minutes (production schedule)
# - After full load completes
# - Continuously for near-real-time updates
# 
# **Input:**
# - Dual watermarks from `fardap_sync_state` (last_watermark_territory, last_watermark_responsible)
# 
# **Output tables:**
# - `fardap_bronze_incidents` - Updated with new/changed records (MERGE mode)
# - `fardap_bronze_cdc_log` - Appended with change tracking
# - `fardap_sync_state` - Updated dual watermarks
# 
# **Duration:** 1-2 minutes (network bound)
# 
# **Key benefits:**
# - ✅ Only fetches changed documents
# - ✅ Captures cross-border incident updates
# - ✅ Idempotent (safe to re-run)
# - ✅ Preserves change history in CDC log
# - ✅ Handles concurrent updates at same timestamp (collision-safe)
# - ✅ Independent watermarks ensure no missed updates


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


def parse_api_datetime(value):
    """Parse API/state timestamps into timezone-aware UTC datetimes."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        iso = value.strip()
        if not iso:
            return None
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(iso)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def to_api_utc_millis(dt):
    """Format datetime as UTC ISO8601 with millisecond precision and trailing Z."""
    dt_utc = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    dt_utc = dt_utc.astimezone(timezone.utc)
    return dt_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")

# Get last watermarks (dual-watermark schema)
DEFAULT_LOOKBACK_MINUTES = 5

try:
    watermark_df = spark.table(TABLE_STATE)
    
    # Check if table has dual-watermark schema
    columns = watermark_df.columns
    has_dual_watermarks = "last_watermark_territory" in columns and "last_watermark_responsible" in columns
    
    if has_dual_watermarks:
        # Dual-watermark schema (post-migration)
        rows = (
            watermark_df
            .select("last_watermark_territory", "last_watermark_responsible")
            .limit(1)
            .collect()
        )
        
        if not rows:
            TERRITORY_FROM_DT = datetime.now(timezone.utc) - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
            RESPONSIBLE_FROM_DT = datetime.now(timezone.utc) - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
            print("📍 No watermarks found, using 5-minute lookback for both")
        else:
            # Parse territory watermark
            territory_value = rows[0]["last_watermark_territory"]
            if territory_value is None:
                TERRITORY_FROM_DT = datetime.now(timezone.utc) - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
                print("⚠️  Territory watermark is NULL, using 5-minute lookback")
            else:
                if isinstance(territory_value, str):
                    iso = territory_value.strip()
                    if iso.endswith("Z"):
                        iso = iso[:-1] + "+00:00"
                    TERRITORY_FROM_DT = datetime.fromisoformat(iso)
                    if TERRITORY_FROM_DT.tzinfo is None:
                        TERRITORY_FROM_DT = TERRITORY_FROM_DT.replace(tzinfo=timezone.utc)
                else:
                    TERRITORY_FROM_DT = territory_value if territory_value.tzinfo else territory_value.replace(tzinfo=timezone.utc)
                print(f"📍 Territory watermark: {TERRITORY_FROM_DT}")
            
            # Parse responsible watermark
            responsible_value = rows[0]["last_watermark_responsible"]
            if responsible_value is None:
                RESPONSIBLE_FROM_DT = datetime.now(timezone.utc) - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
                print("⚠️  Responsible watermark is NULL, using 5-minute lookback")
            else:
                if isinstance(responsible_value, str):
                    iso = responsible_value.strip()
                    if iso.endswith("Z"):
                        iso = iso[:-1] + "+00:00"
                    RESPONSIBLE_FROM_DT = datetime.fromisoformat(iso)
                    if RESPONSIBLE_FROM_DT.tzinfo is None:
                        RESPONSIBLE_FROM_DT = RESPONSIBLE_FROM_DT.replace(tzinfo=timezone.utc)
                else:
                    RESPONSIBLE_FROM_DT = responsible_value if responsible_value.tzinfo else responsible_value.replace(tzinfo=timezone.utc)
                print(f"📍 Responsible watermark: {RESPONSIBLE_FROM_DT}")
    
    else:
        # Legacy single-watermark schema (pre-migration)
        print("⚠️  WARNING: Single-watermark schema detected!")
        print("   Please run migration notebook: Supporting_Scripts/00_Migrate_State_Table")
        print("   Falling back to single watermark for both searches...")
        
        rows = (
            watermark_df
            .select("last_watermark")
            .orderBy(F.col("last_watermark").desc())
            .limit(1)
            .collect()
        )

        if not rows or rows[0][0] is None:
            TERRITORY_FROM_DT = datetime.now(timezone.utc) - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
            RESPONSIBLE_FROM_DT = TERRITORY_FROM_DT
            print("📍 No watermark found, using 5-minute lookback")
        else:
            value = rows[0][0]
            if isinstance(value, str):
                iso = value.strip()
                if iso.endswith("Z"):
                    iso = iso[:-1] + "+00:00"
                UPDATED_FROM_DT = datetime.fromisoformat(iso)
                if UPDATED_FROM_DT.tzinfo is None:
                    UPDATED_FROM_DT = UPDATED_FROM_DT.replace(tzinfo=timezone.utc)
            else:
                UPDATED_FROM_DT = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            
            # Use same watermark for both (legacy behavior)
            TERRITORY_FROM_DT = UPDATED_FROM_DT
            RESPONSIBLE_FROM_DT = UPDATED_FROM_DT
            print(f"📍 Using single watermark for both: {UPDATED_FROM_DT}")

except Exception as e:
    print(f"⚠️  Could not read state table: {e}")
    TERRITORY_FROM_DT = datetime.now(timezone.utc) - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
    RESPONSIBLE_FROM_DT = TERRITORY_FROM_DT
    print("📍 Using 5-minute lookback for both")

# Convert to API format
TERRITORY_FROM = to_api_utc_millis(TERRITORY_FROM_DT)
RESPONSIBLE_FROM = to_api_utc_millis(RESPONSIBLE_FROM_DT)

print(f"\n📍 Search ranges (inclusive):")
print(f"   Territory from:    {TERRITORY_FROM}")
print(f"   Responsible from:  {RESPONSIBLE_FROM}")

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

# Search for changed incidents since watermark with configurable match criteria
def search_with_watermark(match_field, frs_id, updated_from, batch_size=BATCH_SIZE):
    """
    Search for changed incidents by match field since watermark.
    
    Args:
        match_field: Either 'territoryFrsId' or 'responsibleFrsId'
        frs_id: The FRS ID to search for
        updated_from: ISO timestamp string for dateUpdated range filter
        batch_size: Number of results per page
    
    Returns:
        Tuple of (list of document IDs, list of search result objects with dateUpdated)
    """
    url = f'{API_BASE_URL}/api/v1/document/search'
    cursor = None
    ids = []
    search_results = []
    page = 0
    
    print(f'🔍 Searching {match_field}={frs_id} since {updated_from}...')
    
    while True:
        page += 1
        payload = {
            'query': {
                'list': {'documentTypes': ['Incident']},
                'match': {match_field: str(frs_id)},
                'range': [
                    {
                        'from': updated_from,
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
        
        # Extract IDs and keep full results for watermark tracking
        for doc in results:
            doc_id = doc.get('properties', {}).get('documentId')
            if doc_id:
                ids.append(doc_id)
                search_results.append(doc)
        
        new_cursor = data.get('search', {}).get('cursor', {}).get('lastDocumentValues')
        print(f'   📄 Page {page}: +{len([d for d in results if d.get("properties", {}).get("documentId")]):,} changed IDs | Total: {len(ids):,} | Has more: {bool(new_cursor)}')
        
        if not new_cursor or len(results) == 0:
            break
        cursor = new_cursor
    
    print(f'   ✅ {match_field} search complete: {len(ids):,} changed incidents')
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

# Execute dual-search with separate watermarks
print('='*60)
print('🔍 DUAL-SEARCH: Territory + Responsible (Incremental)')
print('='*60)

# Search 1: territoryFrsId (incidents in our territory, changed since territory watermark)
territory_ids, territory_results = search_with_watermark('territoryFrsId', FRS_ID, TERRITORY_FROM)

# Search 2: responsibleFrsId (incidents we're responsible for, changed since responsible watermark)
responsible_ids, responsible_results = search_with_watermark('responsibleFrsId', FRS_ID, RESPONSIBLE_FROM)

# Calculate overlap and deduplicate
territory_set = set(territory_ids)
responsible_set = set(responsible_ids)
all_unique_ids = list(territory_set | responsible_set)

overlap_count = len(territory_set & responsible_set)
territory_only = len(territory_set - responsible_set)
responsible_only = len(responsible_set - territory_set)

print('\n' + '='*60)
print('📊 DUAL-SEARCH SUMMARY (Incremental)')
print('='*60)
print(f'Territory changes:     {len(territory_ids):>8,}')
print(f'Responsible changes:   {len(responsible_ids):>8,}')
print(f'---')
print(f'Overlap:               {overlap_count:>8,}')
print(f'Territory only:        {territory_only:>8,}')
print(f'Responsible only:      {responsible_only:>8,}')
print(f'---')
print(f'Total unique changes:  {len(all_unique_ids):>8,}')
print('='*60)

if len(all_unique_ids) == 0:
    print('\n✅ No changes since last run. Exiting.')
    # Signal downstream pipeline activities to skip when the notebook output carries exitValue=no_changes.
    notebookutils.notebook.exit("no_changes")

# Store results for watermark calculation later
territory_search_results = territory_results
responsible_search_results = responsible_results

# Fetch changed documents in parallel
results = []
completed = 0

print(f'\n📥 Fetching {len(all_unique_ids):,} changed incidents in parallel (MAX_WORKERS={MAX_WORKERS})...')

import concurrent.futures

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

print(f'\n✅ Fetch complete: {len(results):,} / {len(all_unique_ids):,} documents fetched')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build DataFrame
df_raw = spark.createDataFrame(pd.DataFrame(results))
df_raw = df_raw.withColumn('sync_timestamp', F.col('sync_timestamp').cast('timestamp'))
df_raw = df_raw.withColumn('change_ts', F.col('change_ts').cast('timestamp'))
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

# Determine op_type based on whether content actually changed
# Read existing records with their content hashes for comparison
existing_records = spark.table(TABLE_FULL).select('documentId', 'content_hash').cache()

# Find truly new records (not in table at all)
new_records = df_raw.join(existing_records, on='documentId', how='left_anti') \
    .withColumn('op_type', F.lit('insert')) \
    .select('documentId', 'op_type', 'change_ts', 'sync_timestamp')

# Find records that exist but have different content_hash (true updates)
updated_records = df_raw.alias('new') \
    .join(existing_records.alias('old'), on='documentId', how='inner') \
    .where(F.col('new.content_hash') != F.col('old.content_hash')) \
    .select(
        F.col('new.documentId'),
        F.lit('update').alias('op_type'),
        F.col('new.change_ts'),
        F.col('new.sync_timestamp')
    )

# Only track actual changes in CDC
df_tagged = new_records.unionByName(updated_records)

# Cache only if there are changes
cdc_count = df_tagged.count()
unchanged_count = df_raw.count() - cdc_count

if cdc_count > 0:
    df_tagged.cache()
    
    # Diagnostic: Show breakdown of changes
    op_type_breakdown = df_tagged.groupBy('op_type').count().collect()
    print(f'🔍 Change Analysis:')
    for row in op_type_breakdown:
        print(f'   {row.op_type.capitalize()} incidents: {row["count"]}')
        # Show sample IDs
        sample_ids = df_tagged.filter(F.col('op_type') == row.op_type).select('documentId').limit(5).collect()
        if sample_ids:
            print(f'      Sample IDs: {[r.documentId for r in sample_ids]}')
    print(f'   Unchanged incidents (skipped): {unchanged_count}')
else:
    print(f'🔍 Change Analysis:')
    print(f'   No changes detected - all {df_raw.count()} incidents unchanged')

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

# Append to CDC log (only if there were actual changes)
if cdc_count > 0:
    df_cdc = df_tagged

    # Diagnostic: Verify op_type distribution BEFORE writing
    op_type_counts = df_cdc.groupBy('op_type').count().collect()
    print(f'\n📊 CDC DataFrame op_type distribution (BEFORE write):')
    for row in op_type_counts:
        print(f'   {row.op_type}: {row["count"]}')

    print(f'\n📋 Appending {cdc_count:,} records to {TABLE_CDC}...')
    df_cdc.write \
        .format('delta') \
        .mode('append') \
        .option('mergeSchema', 'true') \
        .saveAsTable(TABLE_CDC)

    print(f'✅ CDC log appended')

    # Diagnostic: Verify what actually got written
    recent_cdc = spark.table(TABLE_CDC).orderBy(F.col('sync_timestamp').desc()).limit(100)
    written_counts = recent_cdc.groupBy('op_type').count().collect()
    print(f'\n📊 CDC Table op_type distribution (AFTER write, last 100 records):')
    for row in written_counts:
        print(f'   {row.op_type}: {row["count"]}')
else:
    print(f'\n✅ No changes detected - CDC log unchanged')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate separate watermarks from each search
def calculate_watermark_from_search_results(search_results):
    """Calculate max dateUpdated from search API results."""
    if not search_results:
        return None
    
    max_date_updated = None
    for doc in search_results:
        date_updated = doc.get('properties', {}).get('dateUpdated')
        if date_updated:
            dt = parse_api_datetime(date_updated)
            if dt and (max_date_updated is None or dt > max_date_updated):
                max_date_updated = dt
    
    return max_date_updated

# Calculate territory watermark from territory search results
territory_watermark_dt = calculate_watermark_from_search_results(territory_search_results)
if territory_watermark_dt:
    territory_watermark_str = to_api_utc_millis(territory_watermark_dt)
else:
    # No changes in territory search, keep existing watermark
    territory_watermark_str = TERRITORY_FROM

# Calculate responsible watermark from responsible search results
responsible_watermark_dt = calculate_watermark_from_search_results(responsible_search_results)
if responsible_watermark_dt:
    responsible_watermark_str = to_api_utc_millis(responsible_watermark_dt)
else:
    # No changes in responsible search, keep existing watermark
    responsible_watermark_str = RESPONSIBLE_FROM

# For backwards compatibility, set last_watermark to the max of both
# (deprecated column, but kept to avoid breaking downstream code)
max_watermark_dt = max(
    filter(None, [territory_watermark_dt, responsible_watermark_dt]),
    default=datetime.now(timezone.utc)
)
legacy_watermark_str = to_api_utc_millis(max_watermark_dt)

# Create state table with dual watermarks
df_state = spark.createDataFrame([
    (legacy_watermark_str, territory_watermark_str, responsible_watermark_str)
], ['last_watermark', 'last_watermark_territory', 'last_watermark_responsible'])

print(f'\n🏁 Updating dual watermarks:')
print(f'   Territory watermark:    {territory_watermark_str}')
print(f'   Responsible watermark:  {responsible_watermark_str}')
print(f'   (last_watermark={legacy_watermark_str} for backwards compatibility)')

df_state.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable(TABLE_STATE)

print(f'✅ State table updated with dual watermarks')

# Signal downstream pipeline to skip if no changes detected
if cdc_count == 0:
    print(f'\n🚫 Signaling downstream: no_changes (Silver notebook should skip)')
    notebookutils.notebook.exit("no_changes")

print(f'\n✅ Incremental sync completed with {cdc_count} changes')

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
