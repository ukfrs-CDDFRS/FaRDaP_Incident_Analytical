# Technical Documentation

> Complete technical reference for the FaRDaP Analytical Fabric Ingestion Platform

---

## Table of Contents

- [Bronze Layer](#bronze-layer)
  - [Full Load](#bronze-full-load)
  - [Incremental Sync](#bronze-incremental-sync)
- [Silver Layer](#silver-layer)
  - [Full Transform](#silver-full-transform)
  - [Incremental Transform](#silver-incremental-transform)
- [Semantic Model](#semantic-model)
- [Authentication & Token Management](#authentication--token-management)
- [Code Patterns](#code-patterns)

---

## Bronze Layer

The Bronze layer stores raw JSON documents exactly as received from the FaRDaP API.

### Bronze Full Load

**Notebook:** `01_Bronze_Full_Load.Notebook`

**Purpose:** One-time or occasional full extraction of ALL FaRDaP incident documents.

#### When to Run

| Scenario | Run? |
|:---------|:-----|
| Initial system setup | ✅ Yes |
| Disaster recovery | ✅ Yes |
| Data validation/reconciliation | ✅ Yes |
| Regular updates | ❌ No (use incremental) |

#### Duration

- **Typical:** 30 minutes to several hours
- **Depends on:** Total incident count, API rate limits, network latency

#### Process Flow

```
Step 1: Configuration
├── Load variables from Fabric Variable Library
├── Retrieve secrets from Azure Key Vault
└── Initialize connection parameters

Step 2: Authentication
├── POST /api/v1/auth/init with credentials
├── Extract accessToken and expiresIn from response
├── Calculate token expiry timestamp
├── Store token + expiry (thread-safe)
├── Set User-Agent: Fabric/FaRDaP-Analytical-Platform
└── Log authentication with expiry time

Step 3: Search All Incident IDs
├── POST /api/v1/document/search
├── Query filter: documentTypes=['Incident'], territoryFrsId={FRS_ID}
├── Cursor-based pagination (BATCH_SIZE per page)
└── Collect all documentId values

Step 4: Parallel Document Fetch
├── ThreadPoolExecutor with MAX_WORKERS threads
├── Time-based token refresh (if < 5 min remaining)
├── Count-based token refresh (every REFRESH_EVERY documents)
├── GET /api/v1/document/{documentId}?frsId={FRS_ID}
├── Include User-Agent header on all requests
├── Retry logic: exponential backoff on 429/5xx errors
└── Extract: documentId, raw_json, change_ts, sync_timestamp

Step 5: Create DataFrame
├── Convert results to Spark DataFrame
├── Add content_hash column (SHA-256 of raw_json)
└── Cast timestamps to proper types

Step 6: Write Delta Tables
├── OVERWRITE fardap_bronze_incidents (full table replace)
├── OVERWRITE fardap_bronze_cdc_log (all marked as 'insert')
└── OVERWRITE fardap_sync_state (watermark for incremental)

Step 7: Verification
└── Count records, validate schema, display summary
```

#### Output Tables

| Table | Mode | Description |
|:------|:-----|:------------|
| `fardap_bronze_incidents` | OVERWRITE | All incident documents with raw JSON |
| `fardap_bronze_cdc_log` | OVERWRITE | CDC entries (all 'insert' on full load) |
| `fardap_sync_state` | OVERWRITE | Highest change_ts as watermark |

#### Schema: fardap_bronze_incidents

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | STRING | Unique incident identifier |
| `raw_json` | STRING | Complete JSON document from API |
| `sync_timestamp` | TIMESTAMP | When record was fetched |
| `change_ts` | STRING | API-provided last update timestamp |
| `content_hash` | STRING | SHA-256 hash for change detection |

---

### Bronze Incremental Sync

**Notebook:** `01_Bronze_Incremental_Sync.Notebook`

**Purpose:** Fetch only changed/new documents since the last successful sync.

#### When to Run

| Scenario | Run? |
|:---------|:-----|
| Regular scheduled execution (every 5 min) | ✅ Yes |
| After full load completes | ✅ Yes |
| Near-real-time updates | ✅ Yes |
| Before full load has run | ❌ No |

#### Duration

- **Typical:** 1-2 minutes
- **Depends on:** Number of changes since last sync

#### Key Benefits

- 🚀 Only fetches changed documents (not entire dataset)
- 🔄 Idempotent (safe to re-run multiple times)
- 📋 Preserves complete change history in CDC log
- 💾 Minimal API calls and storage writes

#### Process Flow

```
Step 1: Configuration
└── Same as Full Load

Step 2: Read Last Watermark
├── Query fardap_sync_state table
├── Extract last_watermark timestamp
└── Fallback: 5-minute lookback if not found

Step 3: Authentication
└── Same as Full Load

Step 4: Search Changed IDs
├── POST /api/v1/document/search
├── Query filter: dateUpdated > {last_watermark}
├── Only incidents modified since last sync
└── May return 0 if no changes (exits early)

Step 5: Parallel Document Fetch
└── Same as Full Load (but only for changed IDs)

Step 6: MERGE into Bronze Table
├── Create staging temporary view
├── MERGE INTO fardap_bronze_incidents
├── WHEN MATCHED AND content_hash changed → UPDATE
├── WHEN NOT MATCHED → INSERT
└── Prevents duplicate inserts (idempotent)

Step 7: Append to CDC Log
├── Classify changes: 'update' if change_ts exists, else 'insert'
└── APPEND to fardap_bronze_cdc_log

Step 8: Update Watermark
├── Calculate max(change_ts, sync_timestamp) from this batch
└── OVERWRITE fardap_sync_state
```

#### MERGE Logic

```sql
MERGE INTO fardap_bronze_incidents t
USING staging_incremental s
ON t.documentId = s.documentId
WHEN MATCHED AND t.content_hash <> s.content_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

> **Why Content Hash?** The `content_hash` (SHA-256 of raw_json) ensures we only update records where the actual JSON content changed, not just metadata timestamps.

---

## Silver Layer

The Silver layer contains normalized, business-ready data with flattened columns and separated array tables.

### Silver Full Transform

**Notebook:** `02_Silver_Full_Transform_Enhanced.Notebook`

**Purpose:** Transform ALL Bronze raw JSON into normalized Silver tables with dynamic schema discovery.

#### When to Run

| Scenario | Run? |
|:---------|:-----|
| After initial Bronze Full Load | ✅ Yes |
| When schema changes significantly | ✅ Yes |
| Data quality validation | ✅ Yes |
| Regular updates | ❌ No (use incremental) |

#### Key Features

| Feature | Description |
|:--------|:------------|
| 🔍 Dynamic Array Discovery | Automatically finds ALL arrays in JSON |
| 🔄 Recursive Flattening | Converts nested objects to columns |
| 🆔 Auto-generated IDs | Array rows get `array_id` (PK) + `documentId` (FK) |
| 📊 Content Hash Tracking | Prevents re-flattening unchanged JSON |
| 🔮 Schema Flexibility | Auto-adapts if API adds new fields |

#### Process Flow

```
Step 0: Configuration
├── Load variable library
└── Define table names

Step 1: Helper Functions
├── discover_arrays_in_json(): Recursively finds all arrays
├── flatten_json(): Converts nested dicts to flat key-value pairs
└── extract_arrays_from_json(): Extracts array items with documentId FK

Step 2: Read Bronze + Discover Arrays
├── Load fardap_bronze_incidents
├── Sample 100 records to discover array structures
├── Merge discovered arrays (handles schema variance)
└── Output: ARRAY_TABLES dictionary

Step 3: Flatten JSON to Columns
├── Apply flatten_json UDF to all records
├── Explode map keys to discover ALL unique fields
├── Create column for each discovered field
└── Result: 300+ columns typically

Step 4: Extract Arrays to Normalized Tables
├── For each discovered array type:
│   ├── Extract array items with documentId FK
│   ├── Add array_id (auto-increment) and processed_at
│   └── OVERWRITE to fardap_silver_{array_name}

Step 5: Write Main Silver Table
├── Reorder columns (documentId first)
├── Add metadata (flattened_timestamp, processed_at)
└── OVERWRITE fardap_silver_incidents

Step 6: Track Flattening State
├── Create fardap_silver_flatten_state (watermark)
└── Create fardap_silver_content_hash

Step 7: Create CDC Log
└── OVERWRITE fardap_silver_cdc_log
```

#### Dynamic Array Discovery

```python
# Sample JSON structure discovered:
{
  "victim": [...],              → fardap_silver_victim
  "vehicle": [...],             → fardap_silver_vehicle
  "hazardousmaterial": [...],   → fardap_silver_hazardousmaterial
  "equipment": [...]            → fardap_silver_equipment
}
```

#### Output Tables

| Table | Description |
|:------|:------------|
| `fardap_silver_incidents` | Main flattened table (300+ columns) |
| `fardap_silver_victim` | Casualty/victim information |
| `fardap_silver_vehicle` | Vehicle involvement |
| `fardap_silver_hazardousmaterial` | HAZMAT details |
| `fardap_silver_equipment` | Equipment used |
| `fardap_silver_buildingfacility` | Building information |
| `fardap_silver_system` | System details |
| `fardap_silver_content_hash` | Hash tracking |
| `fardap_silver_flatten_state` | Watermark + mode |
| `fardap_silver_cdc_log` | Change tracking |

---

### Silver Incremental Transform

**Notebook:** `02_Silver_Incremental_Transform_Enhanced.Notebook`

**Purpose:** Incrementally flatten only records where JSON content actually changed.

#### When to Run

| Scenario | Run? |
|:---------|:-----|
| After each Bronze Incremental Sync | ✅ Yes |
| Scheduled after Bronze sync | ✅ Yes |
| Before Silver Full Transform | ❌ No |

#### Duration

- **Typical:** Seconds to minutes
- **Optimization:** ~99% faster than full transform

#### Key Innovation: Content-Hash Filtering

```
Bronze CDC says 100 records changed
       ↓
Content-hash comparison:
- 15 have new/different content_hash → FLATTEN these
- 85 have same content_hash → SKIP (no JSON change)
       ↓
Only 15 records processed!
```

#### Process Flow

```
Step 1: Read Last Flattening Watermark
├── Query fardap_silver_flatten_state
└── Error if not found (run full transform first)

Step 2: Get Changed DocumentIds + Filter by Content Hash
├── Read fardap_bronze_cdc_log for changes > watermark
├── LEFT JOIN with fardap_silver_content_hash
├── Filter: new records OR content_hash changed
└── Skip unchanged JSON (massive performance gain)

Step 3: Flatten Changed Records
├── Apply flatten_json UDF
├── Discover all field keys
└── Create columns for each field

Step 4: MERGE into Silver Main Table
├── Align columns to target schema
├── MERGE: UPDATE matched, INSERT new

Step 5: Update Array Tables
├── For each array type:
│   ├── DELETE existing arrays for changed documentIds
│   └── INSERT new arrays from changed records

Step 6: Update Content Hash Tracking
└── MERGE new content_hash values

Step 7: Append to CDC Log
└── Track which records were updated/inserted

Step 8: Update Flatten State
└── New watermark for next run
```

#### Why DELETE + INSERT for Arrays?

Arrays can have items added, removed, or reordered. A simple MERGE cannot handle this correctly:

1. **DELETE** all existing array items for the changed documentId
2. **INSERT** all current array items from the new JSON

This ensures array tables perfectly reflect the current JSON state.

---

## Semantic Model

The Power BI semantic model uses **Direct Lake** mode for real-time analytics.

### Tables

| Table | Description | Relationship |
|:------|:------------|:-------------|
| `fardap_silver_incidents` | Main incident table | Hub table |
| `fardap_silver_victim` | Casualty details | FK → incidents |
| `fardap_silver_vehicle` | Vehicle involvement | FK → incidents |
| `fardap_silver_hazardousmaterial` | HAZMAT details | FK → incidents |
| `fardap_silver_equipment` | Equipment used | FK → incidents |
| `fardap_silver_buildingfacility` | Building info | FK → incidents |
| `fardap_silver_system` | System information | FK → incidents |
| `fardap_silver_manualsystem` | Manual systems | FK → incidents |
| `fardap_silver_additionalinfo` | Additional info | FK → incidents |
| `fardap_silver_qasummaries` | QA summaries | FK → incidents |
| `fardap_silver_validation` | Validation records | FK → incidents |

### Relationships

All array tables join to incidents via `documentId`:

```tmdl
relationship
    fromColumn: fardap_silver_victim.documentId
    toColumn: fardap_silver_incidents.documentId
```

### Direct Lake Benefits

- ✅ No data import/refresh needed
- ✅ Real-time data from Delta tables
- ✅ Sub-second query performance
- ✅ Automatic schema sync with Lakehouse

---

## Authentication & Token Management

**Updated:** April 27, 2026  
**Applies to:** Bronze Full Load, Bronze Incremental Sync

### Overview

The platform implements a robust authentication system with:
- **Time-based token refresh** - Proactive refresh before expiry
- **Count-based token refresh** - Backup refresh mechanism
- **User-Agent header** - FaRDaP API specification compliance
- **Thread-safe updates** - Safe for parallel processing

### Authentication Flow

```
1. Initial Authentication
   ├── POST /api/v1/auth/init
   ├── Credentials from Azure Key Vault
   ├── Receive: accessToken, expiresIn (3600 seconds)
   └── Calculate: token_expiry = now + expiresIn

2. Token Storage (Thread-Safe)
   ├── Lock acquisition
   ├── Store: shared_token, token_expiry
   └── Lock release

3. Session Creation (Before Each API Call)
   ├── Check: is_token_expiring(buffer=300)?
   │   ├── YES → Re-authenticate
   │   └── NO → Continue
   ├── Create session with current token
   └── Add headers:
       ├── Authorization: Bearer {token}
       ├── Content-Type: application/json
       └── User-Agent: Fabric/FaRDaP-Analytical-Platform/FRS-{FRS_ID}
```

### Token Refresh Mechanisms

#### 1. Time-Based Refresh (Primary)

**Trigger:** Token expires within 5 minutes

```python
def is_token_expiring(buffer_seconds=300):
    """Check if token will expire within buffer_seconds"""
    if token_expiry is None:
        return True
    time_remaining = (token_expiry - datetime.now(timezone.utc)).total_seconds()
    return time_remaining < buffer_seconds

def make_session():
    if is_token_expiring():
        print('🔄 Token expiring soon, refreshing...')
        authenticate()
    # ... create session
```

**Benefits:**
- Prevents 401 errors on long-running jobs
- Proactive (refreshes before expiry, not after)
- Independent of processing speed

#### 2. Count-Based Refresh (Backup)

**Trigger:** Every 25,000 documents processed

```python
if completed % REFRESH_EVERY == 0:
    authenticate()
```

**Benefits:**
- Redundancy if time-based fails
- Prevents "forgetting" to refresh
- Minimal overhead

### Belt-and-Suspenders Approach

Both mechanisms run independently. Whichever triggers first causes refresh:

| Scenario | Time-Based | Count-Based | Winner |
|----------|------------|-------------|--------|
| Fast processing (< 1 hour) | ❌ Not triggered | ✅ Triggers first | Count |
| Slow API (> 1 hour) | ✅ Triggers first | ❌ May not reach count | Time |
| Normal load (mixed speed) | ✅ Usually first | ✅ Backup | Both work |

### User-Agent Header

**Header Value:** `Fabric/FaRDaP-Analytical-Platform/FRS-{FRS_ID}`

**Purpose:**
- FaRDaP API specification compliance
- Fire service identification in API logs
- Client identification for API support
- Rate limiting and usage analytics
- Prevents request rejection

**Implementation:**
```python
s.headers.update({
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
    'User-Agent': f'Fabric/FaRDaP-Analytical-Platform/FRS-{FRS_ID}'
})
```

**Example:** For FRS_ID="17", header becomes `Fabric/FaRDaP-Analytical-Platform/FRS-17`

### Logging

**Authentication Success:**
```
✅ Authenticated successfully (expires at 14:32:15 UTC)
```

**Token Refresh Trigger:**
```
🔄 Token expiring soon, refreshing...
✅ Authenticated successfully (expires at 15:32:15 UTC)
```

**Authentication Failure:**
```
❌ Authentication failed: [error details]
```

### Thread Safety

All token operations are protected by `token_lock`:

```python
token_lock = threading.Lock()

# Writing token
with token_lock:
    shared_token = new_token
    token_expiry = expiry_time

# Reading token
with token_lock:
    token_snapshot = shared_token
```

**Guarantees:**
- No race conditions with 32 parallel workers
- Atomic token updates
- Consistent reads across threads

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `REFRESH_EVERY` | 25,000 | Count-based refresh interval |
| `buffer_seconds` | 300 | Time-based refresh buffer (5 min) |
| `expiresIn` | 3600 | Token lifetime from API (1 hour) |

### Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| "Token expiring soon" every few minutes | API returning short expiry | Check API response `expiresIn` value |
| 401 errors mid-process | Token refresh not triggering | Check logs for refresh attempts |
| Frequent re-authentication | Count too low or buffer too large | Adjust `REFRESH_EVERY` or `buffer_seconds` |
| Missing User-Agent warnings | Header not included | Verify `make_session()` implementation |

### Best Practices

✅ **Do:**
- Monitor logs for token expiry patterns
- Adjust `REFRESH_EVERY` based on your dataset size
- Keep 5-minute buffer for safety
- Include User-Agent on ALL API requests

❌ **Don't:**
- Remove time-based refresh (count-based alone is risky)
- Reduce buffer below 3 minutes (insufficient for retries)
- Hardcode tokens (always use Key Vault)
- Skip User-Agent header (API may reject)

---

## Code Patterns

### Thread-Safe Authentication with Token Expiry

```python
from datetime import datetime, timezone, timedelta

token_lock = threading.Lock()
shared_token = None
token_expiry = None

def authenticate():
    global shared_token, token_expiry
    resp = requests.post(
        f'{API_BASE_URL}/api/v1/auth/init',
        json={'username': USERNAME, 'password': PASSWORD}
    )
    resp.raise_for_status()
    tokens = resp.json().get('tokens', {})
    new_token = tokens.get('accessToken')
    expires_in = tokens.get('expiresIn', 3600)
    
    expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    
    with token_lock:
        shared_token = new_token
        token_expiry = expiry_time
    
    print(f'✅ Authenticated (expires at {expiry_time.strftime("%H:%M:%S UTC")})')

def is_token_expiring(buffer_seconds=300):
    with token_lock:
        if token_expiry is None:
            return True
        return (token_expiry - datetime.now(timezone.utc)).total_seconds() < buffer_seconds

def make_session():
    if is_token_expiring():
        authenticate()
    
    with token_lock:
        token_snapshot = shared_token
    
    s = requests.Session()
    s.headers.update({
        'Authorization': f'Bearer {token_snapshot}',
        'Content-Type': 'application/json',
        'User-Agent': f'Fabric/FaRDaP-Analytical-Platform/FRS-{FRS_ID}'
    })
    return s
```

### Idempotent MERGE Pattern

```sql
MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED AND t.content_hash <> s.content_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### Dynamic Schema Discovery

```python
# Sample multiple records to find all possible fields
all_keys = df.select(F.explode(F.map_keys(F.col("_flat")))).distinct()
for key in all_keys:
    df = df.withColumn(key, F.col("_flat")[key])
```

### Content-Hash Change Detection

```python
df_to_flatten = df_bronze.join(
    df_silver_hashes,
    on="documentId",
    how="left"
).filter(
    F.col("silver.content_hash").isNull() |  # New record
    (F.col("bronze.content_hash") != F.col("silver.content_hash"))  # Changed
)
```

### Variable Library Access

```python
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")
API_BASE_URL = vl.getVariable("API_BASE_URL")
PASSWORD = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-PASSWORD")
```

---

[← Back to README](../README.md)
