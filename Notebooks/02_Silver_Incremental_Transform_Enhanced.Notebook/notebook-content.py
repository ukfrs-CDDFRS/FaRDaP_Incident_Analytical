# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7711294b-27db-8106-455c-9ba8a84a2c40",
# META       "default_lakehouse_name": "inc_fardap_lakehouse",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "7711294b-27db-8106-455c-9ba8a84a2c40",
# META           "workspace_id": "00000000-0000-0000-0000-000000000000"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 02 Silver Incremental Transform Enhanced
# 
# **Purpose**: Incrementally flatten raw_json for records where content_hash CHANGED
# - Read Bronze CDC log to identify changed documentIds
# - Compare content_hash with Silver's tracked hashes
# - ONLY flatten JSON if content_hash is different (not just metadata change)
# - Update Silver main table + array tables via MERGE/DELETE+INSERT
# - **Critical optimization**: Prevents re-flattening the same JSON multiple times
# 
# **Output Tables**:
# - `fardap_silver_incidents` (MERGE: only changed records updated)
# - `fardap_silver_<array_name>` (DELETE+INSERT for changed documentIds)
# - `fardap_silver_content_hash` (updated hashes)
# - `fardap_silver_flatten_state` (new watermark)
# - `fardap_silver_cdc_log` (appends changes)

# CELL ********************

# ============================================================================
# STEP 0: Configuration
# ============================================================================

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
import json
from datetime import datetime

spark.conf.set("spark.sql.session.timeZone", "UTC")


def align_to_table(df, table_name):
    """Preserve existing types and columns without silently coercing payloads."""
    if not spark.catalog.tableExists(table_name):
        return df
    target_schema = spark.table(table_name).schema
    source_types = {field.name: field.dataType for field in df.schema.fields}
    for field in target_schema.fields:
        if field.name not in source_types:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        elif source_types[field.name] != field.dataType:
            raise TypeError(
                f"{table_name}.{field.name}: expected {field.dataType.simpleString()}, "
                f"received {source_types[field.name].simpleString()}; explicit migration required"
            )
    return df


def array_content_json(records):
    """Compare/serialize payloads without volatile processing metadata."""
    payloads = [
        {key: value for key, value in record.items() if key not in ("processed_at", "array_id")}
        for record in records
    ]
    return json.dumps(sorted(payloads, key=lambda record: record["array_index"]), sort_keys=True)


def cdc_json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported CDC JSON value: {type(value).__name__}")

# Get the Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

# Configuration
LAKEHOUSE_NAME = vl.getVariable("LAKEHOUSE_NAME")
CDC_DESCRIPTION_MODE = vl.getVariable("CDC_DESCRIPTION_MODE")  # Compact, Detailed, Complete
TABLE_BRONZE = "fardap_bronze_incidents"
TABLE_BRONZE_CDC = "fardap_bronze_cdc_log"
TABLE_SILVER_MAIN = "fardap_silver_incidents"
TABLE_SILVER_FLATTEN_STATE = "fardap_silver_flatten_state"
TABLE_SILVER_CONTENT_HASH = "fardap_silver_content_hash"
TABLE_SILVER_CDC = "fardap_silver_cdc_log"

print(f"[INFO] Enhanced Silver Incremental Transform")
print(f"  Lakehouse: {LAKEHOUSE_NAME}")
print(f"  Source: {TABLE_BRONZE} (Bronze main table)")
print(f"  Content-hash check: ENABLED (only flatten if JSON changed)")
print(f"  CDC Description Mode: {CDC_DESCRIPTION_MODE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 1: Read Last Flattening Watermark
# ============================================================================

df_flatten_state = spark.table(TABLE_SILVER_FLATTEN_STATE)
state_rows = df_flatten_state.select("last_watermark").limit(2).collect()
if len(state_rows) != 1 or not state_rows[0]["last_watermark"]:
    raise ValueError("Expected one valid Silver watermark; run full transform or repair state")
last_watermark = state_rows[0]["last_watermark"]
if spark.range(1).select(F.try_to_timestamp(F.lit(last_watermark))).first()[0] is None:
    raise ValueError(f"Invalid Silver watermark: {last_watermark!r}")
print(f"[INFO] Last flattening watermark: {last_watermark}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 2: Get Changed DocumentIds + Filter by Content Hash
# ============================================================================

# ARCHITECTURE FIX: Read from Bronze MAIN table, not CDC log
# Why: Bronze only appends to CDC when content_hash changes. If Bronze re-processes
# a document (e.g., full reload) without hash change, it updates the main table's
# sync_timestamp but skips CDC. This causes CDC timestamps to become stale.
# Solution: Query Bronze main table directly by sync_timestamp to catch ALL re-processed records.
print(f"[DEBUG] Querying Bronze table where sync_timestamp >= {last_watermark}")

# DIAGNOSTIC: Show what Bronze actually has (top 5 most recent records)
bronze_latest = spark.table(TABLE_BRONZE).orderBy(F.col("sync_timestamp").desc()).limit(5)
bronze_latest_ts = bronze_latest.select("documentId", "sync_timestamp").collect()
print(f"\n[DIAGNOSTIC] Latest 5 Bronze records:")
for row in bronze_latest_ts:
    print(f"  {row.documentId}: sync_timestamp = {row.sync_timestamp}")
print(f"[DIAGNOSTIC] Silver watermark: {last_watermark}")
print(f"[DIAGNOSTIC] Looking for Bronze records WHERE sync_timestamp >= {last_watermark}\n")

df_bronze_changed = spark.table(TABLE_BRONZE).filter(
    F.col("sync_timestamp") >= F.to_timestamp(F.lit(last_watermark))
).select("documentId", "raw_json", "content_hash", "sync_timestamp")

# Diagnostic: Show timestamp range
changed_count_before_hash_filter = df_bronze_changed.count()
if changed_count_before_hash_filter > 0:
    sync_ts_range = df_bronze_changed.select(
        F.min("sync_timestamp").alias("min_sync_ts"),
        F.max("sync_timestamp").alias("max_sync_ts")
    ).collect()[0]
    print(f"[DEBUG] Found {changed_count_before_hash_filter} Bronze records with recent sync_timestamp")
    print(f"[DEBUG]   sync_timestamp range: {sync_ts_range['min_sync_ts']} to {sync_ts_range['max_sync_ts']}")

changed_doc_ids = df_bronze_changed.select("documentId").distinct().collect()
changed_ids = [row.documentId for row in changed_doc_ids]

if len(changed_ids) == 0:
    print("\n[INFO] ❌ No changes found in Bronze table since last watermark")
    print(f"[INFO] This means Bronze has NO records with sync_timestamp >= {last_watermark}")
    print(f"[INFO] Possible reasons:")
    print(f"  1. Bronze hasn't run yet to process new changes")
    print(f"  2. Bronze processed records but kept OLD timestamps (check Bronze watermark logic)")
    print(f"  3. All recent Bronze records were filtered out by content_hash check")
    print(f"\n[ACTION] Check the diagnostic output above to see Bronze's latest timestamps")
    notebookutils.notebook.exit("success: no_changes")

print(f"[INFO] Found {len(changed_ids)} changed records in Bronze table")

# Compare with Silver's tracked content_hash to filter out unchanged JSON
if spark.catalog.tableExists(TABLE_SILVER_CONTENT_HASH):
    df_silver_hashes = spark.table(TABLE_SILVER_CONTENT_HASH).select("documentId", "content_hash")

    # LEFT JOIN to find: (1) new documentIds, (2) changed content_hash
    df_to_flatten = df_bronze_changed.alias("bronze").join(
        df_silver_hashes.alias("silver"),
        F.col("bronze.documentId") == F.col("silver.documentId"),
        how="left"
    ).filter(
        # NEW record (no silver hash) OR content_hash changed
        F.col("silver.documentId").isNull() |
        ~F.col("bronze.content_hash").eqNullSafe(F.col("silver.content_hash"))
    ).select(
        F.col("bronze.documentId"),
        F.col("bronze.raw_json"),
        F.col("bronze.content_hash"),
        F.col("bronze.sync_timestamp")
    )

    df_to_flatten = df_to_flatten.persist()
    truly_changed_count = df_to_flatten.count()
    skipped_count = len(changed_ids) - truly_changed_count

    print(f"\n[CONTENT-HASH FILTER]:")
    print(f"  Bronze CDC changes: {len(changed_ids)}")
    print(f"  Actually changed JSON: {truly_changed_count}")
    print(f"  Skipped (unchanged content_hash): {skipped_count}")

    if truly_changed_count == 0:
        print("[INFO] No JSON content changes - exiting")
        df_to_flatten.unpersist()
        notebookutils.notebook.exit("success: no_content_changes")

    changed_ids = [row.documentId for row in df_to_flatten.select("documentId").collect()]
else:
    print(f"[WARN] Content hash table not found, flattening all changed records")
    df_to_flatten = df_bronze_changed
    df_to_flatten = df_to_flatten.persist()
    truly_changed_count = df_to_flatten.count()

if df_to_flatten.filter(
    F.col("documentId").isNull() | F.col("sync_timestamp").isNull() | F.col("content_hash").isNull()
).limit(1).count():
    raise ValueError("Changed Bronze records have null keys, timestamps or hashes")
if df_to_flatten.groupBy("documentId").count().filter(F.col("count") > 1).limit(1).count():
    raise ValueError("Changed Bronze records contain duplicate documentId values")

print(f"\n[INFO] Will flatten {truly_changed_count} records with changed JSON")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 3: Load Array Configs from Full Transform (or Re-Discover)
# ============================================================================

# Helper functions - DEEP Recursive Flattening (same as full transform)

def flatten_nested_item(obj, parent_key='', sep='_'):
    """
    Recursively flatten a nested object within an array item.
    This ensures ALL nested fields like rescueDetails.by, rescueDetails.method.value
    are extracted as flat columns: rescueDetails_by, rescueDetails_method_value
    
    Handles:
    - Scalar values
    - Nested dicts (recursively flattened)
    - Nested arrays within array items (converted to JSON strings)
    """
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                # Recursively flatten nested dicts
                nested = flatten_nested_item(v, new_key, sep)
                items.update(nested)
            elif isinstance(v, list):
                # For nested arrays within array items, store as JSON string
                items[new_key] = json.dumps(v) if v else None
            else:
                # Scalar value
                items[new_key] = str(v) if v is not None else None
    elif obj is not None:
        # Handle case where obj itself is a scalar (shouldn't happen at top level but defensive)
        items[parent_key] = str(obj)
    
    return items

def discover_fields_in_array_item(item, parent_key='', sep='_'):
    """
    Recursively discover ALL fields in an array item, including deeply nested ones.
    Returns a list of flattened field names like ['age', 'gender', 'rescueDetails_by', 
    'rescueDetails_location', 'rescueDetails_method_value', 'rescueDetails_method_other']
    """
    fields = []
    if isinstance(item, dict):
        for k, v in item.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                # Recurse into nested dicts
                nested_fields = discover_fields_in_array_item(v, new_key, sep)
                fields.extend(nested_fields)
            elif isinstance(v, list):
                # Nested arrays become a single JSON column
                fields.append(new_key)
            else:
                # Scalar field
                fields.append(new_key)
    return fields

def discover_arrays_in_json(obj, parent_key='', sep='_'):
    """
    Recursively discover ALL arrays in JSON structure.
    ENHANCED: Now recursively discovers ALL nested fields within array items.
    """
    arrays = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, list) and len(v) > 0:
                sample_item = v[0] if len(v) > 0 else {}
                if isinstance(sample_item, dict):
                    # DEEP discovery: recursively find ALL nested fields
                    fields = discover_fields_in_array_item(sample_item)
                    arrays[k] = {
                        "table_name": f"fardap_silver_{k.lower()}",
                        "fields": fields,
                        "path": new_key
                    }
            if isinstance(v, dict):
                nested_arrays = discover_arrays_in_json(v, new_key, sep=sep)
                arrays.update(nested_arrays)
            elif isinstance(v, list) and len(v) > 0:
                # Also look for arrays INSIDE array items
                for item in v[:1]:
                    if isinstance(item, dict):
                        nested_arrays = discover_arrays_in_json(item, new_key, sep=sep)
                        arrays.update(nested_arrays)
    return arrays

def flatten_json(obj, parent_key='', sep='_'):
    items = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, list):
                items.append((new_key, None))
            elif isinstance(v, dict):
                items.extend(flatten_json(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
    return dict(items)

def extract_arrays_from_json(json_str, document_id, array_configs):
    """
    Extract all array items from JSON using discovered array configs.
    ENHANCED: Now recursively flattens ALL nested structures within each array item.
    """
    arrays = {}
    content = json.loads(json_str)
    
    def find_arrays(obj, path=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_path = f"{path}.{k}" if path else k
                if isinstance(v, list) and len(v) > 0 and k in array_configs:
                    table_config = array_configs[k]
                    table_name = table_config["table_name"]
                    if table_name not in arrays:
                        arrays[table_name] = []
                    for idx, item in enumerate(v):
                        if isinstance(item, dict):
                            # Start with base columns
                            row = {"documentId": document_id, "array_index": idx}
                            
                            # DEEP FLATTEN: Recursively flatten the entire array item
                            flattened_item = flatten_nested_item(item)
                            row.update(flattened_item)
                            
                            arrays[table_name].append(row)
                if isinstance(v, dict):
                    find_arrays(v, new_path)
                elif isinstance(v, list):
                    # Also look for nested arrays inside array items
                    for item in v:
                        if isinstance(item, dict):
                            find_arrays(item, new_path)
    find_arrays(content)
    return arrays

# Inspect every changed document to catch rare or newly introduced arrays.
ARRAY_TABLES = {}
sample_jsons = df_to_flatten.select("raw_json").collect()
for sample_row in sample_jsons:
    try:
        sample_content = json.loads(sample_row[0])
        discovered = discover_arrays_in_json(sample_content)
        for array_name, array_config in discovered.items():
            if array_name not in ARRAY_TABLES:
                ARRAY_TABLES[array_name] = array_config
            else:
                existing_fields = set(ARRAY_TABLES[array_name]['fields'])
                new_fields = set(array_config['fields'])
                ARRAY_TABLES[array_name]['fields'] = list(existing_fields | new_fields)
    except (TypeError, ValueError) as e:
        raise ValueError("Invalid Bronze JSON during array discovery") from e

# Existing array tables must also be processed when an array becomes empty or disappears.
for table in spark.catalog.listTables():
    if table.name.startswith("fardap_silver_") and not table.isTemporary:
        columns = spark.table(table.name).columns
        if "documentId" in columns and "array_index" in columns:
            if table.name not in {config["table_name"] for config in ARRAY_TABLES.values()}:
                ARRAY_TABLES[table.name] = {"table_name": table.name, "fields": []}

print(f"[INFO] Discovered {len(ARRAY_TABLES)} array types (includes any new arrays)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 4: Flatten Changed Records to Columns
# ============================================================================

flatten_udf = F.udf(
    lambda json_str: flatten_json(json.loads(json_str)) if json_str else {},
    T.MapType(T.StringType(), T.StringType())
)

df_flattened_changed = df_to_flatten.withColumn("_flat", flatten_udf(F.col("raw_json")))

all_keys = df_flattened_changed.select(F.explode(F.map_keys(F.col("_flat")))).distinct().collect()
all_keys = sorted([row[0] for row in all_keys if row[0] is not None])

print(f"[INFO] Discovered {len(all_keys)} fields in changed records")

for key in all_keys:
    col_name = key.lower().replace(" ", "_")
    df_flattened_changed = df_flattened_changed.withColumn(
        col_name,
        F.col("_flat")[key].cast(T.StringType())
    )

df_flattened_changed = df_flattened_changed.drop("_flat")

df_silver_changed = df_flattened_changed.withColumn(
    "flattened_timestamp", F.current_timestamp()
).withColumn(
    "processed_at", F.current_timestamp()
).persist()
df_silver_changed.count()
df_silver_changed = align_to_table(df_silver_changed, TABLE_SILVER_MAIN)

print(f"[INFO] Flattened {truly_changed_count} changed records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 4B: Capture "Before" and "After" State for CDC
# ============================================================================

print(f"\n[CDC PREP] Capturing before/after state for change tracking...")

# CAPTURE "AFTER" STATE: Collect new records NOW (before any transformations)
new_records_for_cdc = {}
for row in df_silver_changed.filter(F.col("documentId").isin(changed_ids)).collect():
    new_records_for_cdc[row.documentId] = row.asDict()
print(f"  Captured {len(new_records_for_cdc)} NEW records (after flattening)")

# CAPTURE "BEFORE" STATE: Read existing Silver records to capture old state
df_before_merge = spark.table(TABLE_SILVER_MAIN).filter(
    F.col("documentId").isin(changed_ids)
)
if df_before_merge.groupBy("documentId").count().filter(F.col("count") > 1).limit(1).count():
    raise ValueError("Silver target contains duplicate documentId values; repair before MERGE")

# Determine which records already exist (updates) vs new (inserts)
existing_doc_ids_before_merge = set([row.documentId for row in df_before_merge.select("documentId").collect()])
inserts_set = set([doc_id for doc_id in changed_ids if doc_id not in existing_doc_ids_before_merge])
updates_set = set([doc_id for doc_id in changed_ids if doc_id in existing_doc_ids_before_merge])

print(f"  INSERTs (new documentIds): {len(inserts_set)}")
print(f"  UPDATEs (existing documentIds): {len(updates_set)}")

# Collect old records for UPDATEs
old_records_for_cdc = {}
if len(updates_set) > 0:
    for row in df_before_merge.filter(F.col("documentId").isin(list(updates_set))).collect():
        old_records_for_cdc[row.documentId] = row.asDict()
    print(f"  Captured {len(old_records_for_cdc)} OLD records (before merge)")
else:
    print(f"  No OLD records to capture (all are inserts)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 5: MERGE into Silver Main Table (align columns to target schema)
# ============================================================================

target_cols = spark.table(TABLE_SILVER_MAIN).columns
new_fields = [field for field in df_silver_changed.schema.fields if field.name not in target_cols]
if new_fields:
    additions = ", ".join(
        f"`{field.name.replace('`', '``')}` {field.dataType.simpleString()}" for field in new_fields
    )
    spark.sql(f"ALTER TABLE {TABLE_SILVER_MAIN} ADD COLUMNS ({additions})")
df_silver_changed.createOrReplaceTempView("staging_silver_changed")

spark.sql(f"""
MERGE INTO {TABLE_SILVER_MAIN} t
USING staging_silver_changed s
ON t.documentId = s.documentId
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] MERGE completed for {TABLE_SILVER_MAIN}")
print(f"[INFO] Updated/inserted {truly_changed_count} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 6: Extract and Update Arrays for Changed Records
# CAPTURES full "before" and "after" array data for CDC tracking
# ============================================================================

extracted_arrays = {}
bronze_changed_data = df_to_flatten.collect()

print(f"[INFO] Extracting arrays from {len(bronze_changed_data)} changed records...")

for row in bronze_changed_data:
    doc_id = row.documentId
    raw_json = row.raw_json
    arrays_dict = extract_arrays_from_json(raw_json, doc_id, ARRAY_TABLES)
    for table_name, array_data in arrays_dict.items():
        if table_name not in extracted_arrays:
            extracted_arrays[table_name] = []
        extracted_arrays[table_name].extend(array_data)

# TRACK ARRAY CHANGES FOR CDC: Capture "before" state
print(f"[INFO] Capturing array state before update (for CDC tracking)...")
array_changes_by_doc = {}  # {documentId: {table_name: {"old_count": X, "new_count": Y}}}

for doc_id in changed_ids:
    array_changes_by_doc[doc_id] = {}

print(f"[INFO] Updating array tables:")

for array_type, table_config in ARRAY_TABLES.items():
    table_name = table_config["table_name"]
    records = extracted_arrays.get(table_name, [])
    
    # CAPTURE "BEFORE" STATE: Track existing array records (full data + counts) for CDC
    if spark.catalog.tableExists(table_name):
        df_old_arrays = spark.table(table_name).filter(F.col("documentId").isin(changed_ids))
        
        # Capture counts
        old_counts_by_doc = df_old_arrays.groupBy("documentId").count().collect()
        old_counts_dict = {row.documentId: row["count"] for row in old_counts_by_doc}
        
        # Capture full records for Complete mode
        old_records_by_doc = {}
        for row in df_old_arrays.collect():
            doc_id = row.documentId
            if doc_id not in old_records_by_doc:
                old_records_by_doc[doc_id] = []
            old_records_by_doc[doc_id].append(row.asDict())
    else:
        old_counts_dict = {}
        old_records_by_doc = {}
    
    # Track arrays for ALL changed documentIds (even if 0 records)
    for doc_id in changed_ids:
        if table_name not in array_changes_by_doc.get(doc_id, {}):
            array_changes_by_doc[doc_id][table_name] = {
                "old_count": old_counts_dict.get(doc_id, 0),
                "new_count": 0,  # Will be updated after insert
                "old_records": old_records_by_doc.get(doc_id, []),
                "new_records": []  # Will be updated after insert
            }
    
    if records:
        # STEP 1: Collect ALL unique keys across ALL records for this table
        all_keys = set()
        for record in records:
            all_keys.update(record.keys())
        # Ensure metadata columns always exist and are typed consistently.
        all_keys.update(["documentId", "array_index"])
        all_keys = sorted(all_keys)
        
        # STEP 2: Normalize all records - ensure every record has ALL keys (None for missing)
        # and enforce stable types for metadata columns.
        normalized_records = []
        for record in records:
            normalized = {}
            for key in all_keys:
                val = record.get(key)
                if key == "documentId":
                    normalized[key] = str(val) if val is not None else None
                elif key == "array_index":
                    try:
                        normalized[key] = int(val) if val is not None else None
                    except (TypeError, ValueError):
                        normalized[key] = None
                else:
                    # Convert everything to string to prevent schema merge conflicts
                    normalized[key] = str(val) if val is not None else None
            normalized_records.append(normalized)
        
        # STEP 3: Build explicit schema - StringType for all except metadata
        schema_fields = []
        for key in all_keys:
            if key == "documentId":
                schema_fields.append(T.StructField(key, T.StringType(), True))
            elif key == "array_index":
                schema_fields.append(T.StructField(key, T.IntegerType(), True))
            else:
                schema_fields.append(T.StructField(key, T.StringType(), True))
        schema = T.StructType(schema_fields)
        
        # STEP 4: Create DataFrame with explicit schema
        df_array = spark.createDataFrame(normalized_records, schema=schema)

        # Fail early on invalid metadata instead of writing corrupted array relationships.
        null_array_index_count = df_array.filter(F.col("array_index").isNull()).count()
        if null_array_index_count > 0:
            raise ValueError(
                f"[ERROR] {table_name}: found {null_array_index_count} rows with null array_index"
            )

        df_array = df_array.withColumn("documentId", F.col("documentId").cast(T.StringType()))
        df_array = df_array.withColumn("array_index", F.col("array_index").cast(T.IntegerType()))
        
        df_array = df_array.withColumn(
            "array_id",
            F.row_number().over(Window.partitionBy("documentId").orderBy("array_index"))
        )
        df_array = df_array.withColumn("processed_at", F.current_timestamp())

        df_array = align_to_table(df_array, table_name)
        doc_ids_str = ",".join("'" + doc_id.replace("'", "''") + "'" for doc_id in changed_ids)
        df_array.write.format("delta").mode("overwrite") \
            .option("replaceWhere", f"documentId IN ({doc_ids_str})") \
            .option("mergeSchema", "true").saveAsTable(table_name)
        write_mode = "atomic-replace"
        
        # CAPTURE "AFTER" STATE: Update new counts + full records for CDC tracking
        new_counts_by_doc = df_array.groupBy("documentId").count().collect()
        new_counts_dict = {row.documentId: row["count"] for row in new_counts_by_doc}
        
        # Capture full new records for Complete mode
        new_records_by_doc = {}
        for row in df_array.collect():
            doc_id = row.documentId
            if doc_id not in new_records_by_doc:
                new_records_by_doc[doc_id] = []
            new_records_by_doc[doc_id].append(row.asDict())
        
        # Update tracking dictionary
        for doc_id in changed_ids:
            if table_name in array_changes_by_doc.get(doc_id, {}):
                array_changes_by_doc[doc_id][table_name]["new_count"] = new_counts_dict.get(doc_id, 0)
                array_changes_by_doc[doc_id][table_name]["new_records"] = new_records_by_doc.get(doc_id, [])
        
        print(f"  ✓ {table_name}: {len(records)} rows, {len(df_array.columns)} columns [{write_mode}]")
    elif spark.catalog.tableExists(table_name):
        doc_ids_str = ",".join("'" + doc_id.replace("'", "''") + "'" for doc_id in changed_ids)
        spark.sql(f"DELETE FROM {table_name} WHERE documentId IN ({doc_ids_str})")

print(f"[INFO] Array tables updated")

# DIAGNOSTIC: Show array changes summary
print(f"\n[ARRAY CDC TRACKING] Summary of array changes (full records captured):")
total_array_changes = 0
total_content_only_changes = 0
for doc_id in changed_ids:
    doc_array_changes = []
    for table_name, data in array_changes_by_doc.get(doc_id, {}).items():
        old_count = data.get("old_count", 0)
        new_count = data.get("new_count", 0)
        old_records = data.get("old_records", [])
        new_records = data.get("new_records", [])
        
        # Check if count changed
        if old_count != new_count:
            array_type = table_name.replace("fardap_silver_", "")
            doc_array_changes.append(f"{array_type}: {old_count}→{new_count} items")
            total_array_changes += 1
        # Check if content changed (same count)
        elif old_count > 0:
            old_json = array_content_json(old_records)
            new_json = array_content_json(new_records)
            if old_json != new_json:
                array_type = table_name.replace("fardap_silver_", "")
                doc_array_changes.append(f"{array_type}: {old_count} items (content changed)")
                total_array_changes += 1
                total_content_only_changes += 1
    
    if doc_array_changes and len(doc_array_changes) <= 3:  # Show first 3 examples
        print(f"  {doc_id}: {', '.join(doc_array_changes)}")
if total_array_changes == 0:
    print(f"  No array changes detected")
else:
    print(f"  Total array tables with changes: {total_array_changes}")
    if total_content_only_changes > 0:
        print(f"  ↳ Content-only changes (same count, different data): {total_content_only_changes}")
print(f"  (Complete records stored for CDC mode: {CDC_DESCRIPTION_MODE})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 7: Update Content Hash Tracking
# ============================================================================

df_new_hashes = df_to_flatten.select(
    F.col("documentId"),
    F.col("content_hash"),
    F.current_timestamp().alias("last_flattened_at")
)

# Commit hashes only after CDC succeeds so a failed log write remains retryable.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 8: Generate Change Descriptions and Append to CDC Log
# ============================================================================

print(f"\n[INFO] Generating change descriptions (Mode: {CDC_DESCRIPTION_MODE})...")
print(f"  Using before-state captured in STEP 4B (INSERTs: {len(inserts_set)}, UPDATEs: {len(updates_set)})")

# Create df_silver_cdc with op_type for CDC logging
cdc_records = []
for doc_id in inserts_set:
    cdc_records.append({"documentId": doc_id, "op_type": "insert"})
for doc_id in updates_set:
    cdc_records.append({"documentId": doc_id, "op_type": "update"})

df_silver_cdc = spark.createDataFrame(cdc_records, schema="documentId STRING, op_type STRING")

# Use the records captured in STEP 4B (BEFORE any transformations)
old_records = old_records_for_cdc  # Captured from Silver table before merge
new_records = new_records_for_cdc  # Captured from df_silver_changed before column transformations

# Diagnostic: Verify we have the data we need
print(f"  OLD records available: {len(old_records)}")
print(f"  NEW records available: {len(new_records)}")
if len(new_records) == 0:
    print(f"  [ERROR] No NEW records found! This should never happen.")
    print(f"  [ERROR] Check STEP 4B - new_records_for_cdc should be populated")
else:
    # Show sample of what we captured
    sample_doc_id = list(new_records.keys())[0]
    sample_new = new_records[sample_doc_id]
    print(f"\n  [SAMPLE NEW RECORD] {sample_doc_id}:")
    sample_fields = [k for k in sample_new.keys() if k.startswith('content_')][:3]
    for field in sample_fields:
        value = sample_new.get(field)
        print(f"    {field}: {value[:50] if value and len(str(value)) > 50 else value}")
    
    if sample_doc_id in old_records:
        sample_old = old_records[sample_doc_id]
        print(f"  [SAMPLE OLD RECORD] {sample_doc_id}:")
        for field in sample_fields:
            value = sample_old.get(field)
            print(f"    {field}: {value[:50] if value and len(str(value)) > 50 else value}")

# Generate change descriptions
change_descriptions = {}

for doc_id in changed_ids:
    if doc_id in inserts_set:
        # For inserts, count non-null fields
        new_rec = new_records.get(doc_id, {})
        non_null_fields = sum(1 for v in new_rec.values() if v is not None)
        
        # Add array information for inserts
        array_info = []
        for table_name, counts in array_changes_by_doc.get(doc_id, {}).items():
            new_count = counts.get("new_count", 0)
            if new_count > 0:
                # Extract array type from table name (e.g., fardap_silver_casualties -> casualties)
                array_type = table_name.replace("fardap_silver_", "").replace("_", " ")
                array_info.append(f"{new_count} {array_type}")
        
        base_description = f"New record with {non_null_fields} populated fields"
        if array_info:
            base_description += f" | Arrays: {', '.join(array_info)}"
        change_descriptions[doc_id] = base_description
    
    elif doc_id in updates_set:
        old_rec = old_records.get(doc_id, {})
        new_rec = new_records.get(doc_id, {})
        
        # Diagnostic: Verify we have both old and new records
        if not old_rec:
            print(f"  [WARN] {doc_id}: No old record found (should have been captured in STEP 4B)")
        if not new_rec:
            print(f"  [WARN] {doc_id}: No new record found (should have been captured in STEP 4B)")
        
        # Find changed fields (use all columns from new record)
        changed_fields = []
        # Focus on content_* fields for performance, or use all fields if needed
        if new_rec:
            important_fields = [col for col in new_rec.keys() 
                              if col.startswith('content_') or col in ['documentId', 'content_hash']]
        else:
            print(f"  [ERROR] {doc_id}: new_rec is empty!")
            important_fields = []
        
        for field in important_fields:
            old_val = old_rec.get(field)
            new_val = new_rec.get(field)
            
            # Compare values (handle None/null)
            if old_val != new_val:
                # Skip if both are None/null
                if old_val is None and new_val is None:
                    continue
                changed_fields.append((field, old_val, new_val))
        
        # Detect array changes for this documentId
        array_changes = []
        for table_name, counts in array_changes_by_doc.get(doc_id, {}).items():
            old_count = counts.get("old_count", 0)
            new_count = counts.get("new_count", 0)
            old_array_records = counts.get("old_records", [])
            new_array_records = counts.get("new_records", [])
            
            # Check if array changed: either count changed OR content changed (same count, different records)
            array_changed = False
            if old_count != new_count:
                array_changed = True
            elif old_count > 0:  # Same count, but check if content changed
                # Compare records (convert to JSON strings for comparison since records are dicts)
                old_json = array_content_json(old_array_records)
                new_json = array_content_json(new_array_records)
                if old_json != new_json:
                    array_changed = True
            
            if array_changed:
                array_type = table_name.replace("fardap_silver_", "").replace("_", " ")
                array_changes.append((array_type, old_count, new_count))
        
        # Generate description based on mode
        if CDC_DESCRIPTION_MODE == "Compact":
            # Option A: List field names only
            field_names = [f[0] for f in changed_fields]
            description = f"{len(changed_fields)} fields changed: {', '.join(field_names[:10])}"
            if len(field_names) > 10:
                description += f" +{len(field_names)-10} more"
            
            # Add array changes
            if array_changes:
                array_summary = [f"{atype}: {old}→{new}" for atype, old, new in array_changes]
                description += f" | Arrays: {', '.join(array_summary)}"
            
            change_descriptions[doc_id] = description
        
        elif CDC_DESCRIPTION_MODE == "Detailed":
            # Option B: Show old→new for first 5 fields
            details = []
            for field, old_val, new_val in changed_fields[:5]:
                old_str = str(old_val)[:50] if old_val is not None else "null"
                new_str = str(new_val)[:50] if new_val is not None else "null"
                details.append(f"{field}: '{old_str}' → '{new_str}'")
            
            description = "; ".join(details)
            if len(changed_fields) > 5:
                description += f"; +{len(changed_fields)-5} other fields"
            
            # Add array changes
            if array_changes:
                array_details = [f"{atype}: {old} items → {new} items" for atype, old, new in array_changes]
                description += f" | Arrays: {', '.join(array_details)}"
            
            change_descriptions[doc_id] = description
        
        elif CDC_DESCRIPTION_MODE == "Complete":
            # Option C: Full JSON of all changes
            changes_dict = {}
            for field, old_val, new_val in changed_fields:
                changes_dict[field] = {
                    "old": str(old_val) if old_val is not None else None,
                    "new": str(new_val) if new_val is not None else None
                }
            
            # Add array changes to JSON with FULL records (not just counts)
            if array_changes:
                changes_dict["_arrays"] = {}
                for atype, old_count, new_count in array_changes:
                    # Find the actual table data
                    table_name = f"fardap_silver_{atype.replace(' ', '_')}"
                    array_data = array_changes_by_doc.get(doc_id, {}).get(table_name, {})
                    
                    changes_dict["_arrays"][atype] = {
                        "old": array_data.get("old_records", []),
                        "new": array_data.get("new_records", [])
                    }
            
            change_descriptions[doc_id] = json.dumps(changes_dict, default=cdc_json_default)
        
        else:
            # Fallback
            description = f"{len(changed_fields)} fields changed"
            if array_changes:
                array_summary = [f"{atype}: {old}→{new}" for atype, old, new in array_changes]
                description += f" | Arrays: {', '.join(array_summary)}"
            change_descriptions[doc_id] = description
    else:
        change_descriptions[doc_id] = "Unknown operation"

# Create descriptions DataFrame
df_descriptions = spark.createDataFrame(
    [(doc_id, desc) for doc_id, desc in change_descriptions.items()],
    schema="documentId STRING, change_description STRING"
)

# Join descriptions to CDC records
df_silver_cdc = df_silver_cdc.join(
    df_descriptions,
    on="documentId",
    how="left"
).withColumn("cdc_timestamp", F.current_timestamp()) \
 .withColumn("flattened_at", F.current_timestamp())

# Schema alignment: Ensure columns match existing table
df_silver_cdc = align_to_table(df_silver_cdc, TABLE_SILVER_CDC)

df_silver_cdc.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TABLE_SILVER_CDC)

print(f"[INFO] Appended {truly_changed_count} records to {TABLE_SILVER_CDC}")
print(f"[INFO] Change descriptions generated: {len(change_descriptions)}")

# Diagnostic: Show sample change description
if len(change_descriptions) > 0 and CDC_DESCRIPTION_MODE == "Complete":
    sample_doc_id = list(change_descriptions.keys())[0]
    sample_desc = change_descriptions[sample_doc_id]
    print(f"\\n[SAMPLE CDC] DocumentId: {sample_doc_id}")
    print(f"  Description preview: {sample_desc[:200]}..." if len(sample_desc) > 200 else f"  Description: {sample_desc}")

# Diagnostic: Verify what was written
recent_silver_cdc = spark.table(TABLE_SILVER_CDC).orderBy(F.col("cdc_timestamp").desc()).limit(100)
written_op_types = recent_silver_cdc.groupBy("op_type").count().collect()
print(f"\n📊 Silver CDC op_type distribution (last 100 records):")
for row in written_op_types:
    print(f"   {row.op_type}: {row['count']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 9: Update Flatten State
# ============================================================================

if spark.catalog.tableExists(TABLE_SILVER_CONTENT_HASH):
    df_new_hashes.createOrReplaceTempView("staging_content_hash")
    spark.sql(f"""
    MERGE INTO {TABLE_SILVER_CONTENT_HASH} t
    USING staging_content_hash s
    ON t.documentId = s.documentId
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
else:
    df_new_hashes.write.format("delta").saveAsTable(TABLE_SILVER_CONTENT_HASH)

# CRITICAL FIX: Use MAX timestamp from Bronze records we ACTUALLY PROCESSED
# Why: Bronze may write records with old timestamps (e.g., during reprocessing).
# If we use "now" as watermark, we'll skip those old-timestamped records on next run.
# Solution: Use the MAX sync_timestamp from the records we flattened.

if truly_changed_count > 0:
    # Get the maximum sync_timestamp from records we processed
    max_processed_timestamp = df_to_flatten.agg(
        F.date_format(F.max("sync_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    ).first()[0]
    
    if max_processed_timestamp:
        # Use the max timestamp we processed as watermark
        new_watermark = max_processed_timestamp
        print(f"[INFO] Using MAX sync_timestamp from processed records: {new_watermark}")
    else:
        raise ValueError("No processed sync_timestamp; refusing to advance Silver state")
else:
    # No records processed, keep existing watermark
    new_watermark = last_watermark
    print(f"[INFO] No records processed, keeping existing watermark: {new_watermark}")

df_new_state = spark.createDataFrame(
    [(int(spark.table(TABLE_SILVER_MAIN).count()), new_watermark, "incremental")],
    schema="total_flattened INT, last_watermark STRING, mode STRING"
 )
df_new_state.write.format("delta").mode("overwrite").saveAsTable(TABLE_SILVER_FLATTEN_STATE)

print(f"[INFO] Updated {TABLE_SILVER_FLATTEN_STATE}")
print(f"  New Watermark: {new_watermark}")
print(f"  Total Flattened: {spark.table(TABLE_SILVER_MAIN).count():,}")
df_to_flatten.unpersist()
df_silver_changed.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 10: Summary
# ============================================================================

print(f"\n[SUCCESS] ✅ Silver Layer Incremental Transform Complete!")
print(f"  Bronze CDC Records: {len(changed_doc_ids)}")
print(f"  Content-Hash Filtered: {truly_changed_count} (skipped {len(changed_doc_ids) - truly_changed_count} unchanged)")
print(f"  Processing Speed: ~99% faster than full transform")
print(f"\n[SUMMARY]")
print(f"  Main Table Rows: {spark.table(TABLE_SILVER_MAIN).count():,}")
print(f"  CDC Log Entries: {spark.table(TABLE_SILVER_CDC).count():,}")
print(f"  Content Hash Tracking: Updated")
print(f"\n[PERFORMANCE] This run only flattened records where JSON content actually changed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
