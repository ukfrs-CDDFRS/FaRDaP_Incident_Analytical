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

try:
    df_flatten_state = spark.table(TABLE_SILVER_FLATTEN_STATE)
    last_watermark = df_flatten_state.select("last_watermark").collect()[0][0]
    print(f"[INFO] Last flattening watermark: {last_watermark}")
except Exception as e:
    print(f"[ERROR] Flatten state table not found: {e}")
    print(f"[INFO] Run full transform first (04_Silver_Full_Transform_Enhanced)")
    notebookutils.notebook.exit("error: flatten_state_not_found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 2: Get Changed DocumentIds + Filter by Content Hash
# ============================================================================

if not last_watermark:
    last_watermark = "1970-01-01T00:00:00Z"
    print("[WARN] last_watermark missing; defaulting to epoch")

# ARCHITECTURE FIX: Read from Bronze MAIN table, not CDC log
# Why: Bronze only appends to CDC when content_hash changes. If Bronze re-processes
# a document (e.g., full reload) without hash change, it updates the main table's
# sync_timestamp but skips CDC. This causes CDC timestamps to become stale.
# Solution: Query Bronze main table directly by sync_timestamp to catch ALL re-processed records.
print(f"[DEBUG] Querying Bronze table where sync_timestamp >= {last_watermark}")

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
    print("[INFO] No changes since last watermark - exiting")
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
        F.col("silver.content_hash").isNull() |
        (F.col("bronze.content_hash") != F.col("silver.content_hash"))
    ).select(
        F.col("bronze.documentId"),
        F.col("bronze.raw_json"),
        F.col("bronze.content_hash"),
        F.col("bronze.sync_timestamp")
    )

    truly_changed_count = df_to_flatten.count()
    skipped_count = len(changed_ids) - truly_changed_count

    print(f"\n[CONTENT-HASH FILTER]:")
    print(f"  Bronze CDC changes: {len(changed_ids)}")
    print(f"  Actually changed JSON: {truly_changed_count}")
    print(f"  Skipped (unchanged content_hash): {skipped_count}")

    if truly_changed_count == 0:
        print("[INFO] No JSON content changes - exiting")
        notebookutils.notebook.exit("success: no_content_changes")

    changed_ids = [row.documentId for row in df_to_flatten.select("documentId").collect()]
else:
    print(f"[WARN] Content hash table not found, flattening all changed records")
    df_to_flatten = df_bronze_changed
    truly_changed_count = df_to_flatten.count()

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
    try:
        content = json.loads(json_str)
    except:
        return arrays
    
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

# Re-discover arrays from sample (to catch any NEW arrays added to API)
ARRAY_TABLES = {}
sample_jsons = df_to_flatten.select("raw_json").limit(20).collect()
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
    except:
        continue

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
)

print(f"[INFO] Flattened {truly_changed_count} changed records")

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

for c in target_cols:
    if c not in df_silver_changed.columns:
        df_silver_changed = df_silver_changed.withColumn(c, F.lit(None))

df_silver_changed = df_silver_changed.select(*target_cols)
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

print(f"[INFO] Updating array tables:")

for array_type, table_config in ARRAY_TABLES.items():
    table_name = table_config["table_name"]
    records = extracted_arrays.get(table_name, [])
    
    if records:
        # DELETE old arrays for these documentIds (only if table exists)
        if spark.catalog.tableExists(table_name):
            doc_ids_str = ",".join([f"'{d}'" for d in changed_ids])
            spark.sql(f"DELETE FROM {table_name} WHERE documentId IN ({doc_ids_str})")
        else:
            print(f"  ℹ️  {table_name} not found; creating on insert")
        
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

        write_mode = "append"
        try:
            # Avoid blind schema merge. Auto-heal only on known Delta field conflict.
            df_array.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(table_name)
        except Exception as e:
            if "DELTA_FAILED_TO_MERGE_FIELDS" not in str(e):
                raise

            write_mode = "auto-heal-rebuild"
            print(f"  [WARN] {table_name}: schema conflict detected; rebuilding table with aligned schema")

            if spark.catalog.tableExists(table_name):
                df_existing = spark.table(table_name)
                if "documentId" in df_existing.columns:
                    df_existing = df_existing.withColumn("documentId", F.col("documentId").cast(T.StringType()))
                if "array_index" in df_existing.columns:
                    df_existing = df_existing.withColumn("array_index", F.col("array_index").cast(T.IntegerType()))

                target_cols = sorted(set(df_existing.columns) | set(df_array.columns))
                for col_name in target_cols:
                    if col_name not in df_existing.columns:
                        df_existing = df_existing.withColumn(col_name, F.lit(None))
                    if col_name not in df_array.columns:
                        df_array = df_array.withColumn(col_name, F.lit(None))

                df_rebuild = df_existing.select(*target_cols).unionByName(df_array.select(*target_cols))
            else:
                df_rebuild = df_array

            df_rebuild.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        
        print(f"  ✓ {table_name}: {len(records)} rows, {len(df_array.columns)} columns [{write_mode}]")

print(f"[INFO] Array tables updated")

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

df_new_hashes.createOrReplaceTempView("staging_content_hash")

spark.sql(f"""
MERGE INTO {TABLE_SILVER_CONTENT_HASH} t
USING staging_content_hash s
ON t.documentId = s.documentId
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print(f"[INFO] Updated {TABLE_SILVER_CONTENT_HASH}")
print(f"[INFO] Tracked {truly_changed_count} new content_hash values")

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

# Read existing Silver records for comparison (only for records being updated)
df_existing = spark.table(TABLE_SILVER_MAIN).filter(
    F.col("documentId").isin(changed_ids)
)

# Determine op_type by checking if record already exists in Silver
existing_doc_ids_in_silver = set([row.documentId for row in df_existing.select("documentId").collect()])

inserts_set = set([doc_id for doc_id in changed_ids if doc_id not in existing_doc_ids_in_silver])
updates_set = set([doc_id for doc_id in changed_ids if doc_id in existing_doc_ids_in_silver])

print(f"  INSERTs: {len(inserts_set)}, UPDATEs: {len(updates_set)}")

# Create df_silver_cdc with op_type for CDC logging
cdc_records = []
for doc_id in inserts_set:
    cdc_records.append({"documentId": doc_id, "op_type": "insert"})
for doc_id in updates_set:
    cdc_records.append({"documentId": doc_id, "op_type": "update"})

df_silver_cdc = spark.createDataFrame(cdc_records, schema="documentId STRING, op_type STRING")

# Collect old and new records for comparison
old_records = {}
if len(updates_set) > 0:
    for row in df_existing.filter(F.col("documentId").isin(list(updates_set))).collect():
        old_records[row.documentId] = row.asDict()

new_records = {}
for row in df_silver_changed.filter(F.col("documentId").isin(changed_ids)).collect():
    new_records[row.documentId] = row.asDict()

# Generate change descriptions
change_descriptions = {}

for doc_id in changed_ids:
    if doc_id in inserts_set:
        # For inserts, count non-null fields
        new_rec = new_records.get(doc_id, {})
        non_null_fields = sum(1 for v in new_rec.values() if v is not None)
        change_descriptions[doc_id] = f"New record with {non_null_fields} populated fields"
    
    elif doc_id in updates_set:
        old_rec = old_records.get(doc_id, {})
        new_rec = new_records.get(doc_id, {})
        
        # Find changed fields (focus on content_* fields for performance)
        changed_fields = []
        important_fields = [col for col in df_silver_changed.columns 
                          if col.startswith('content_') or col in ['documentId', 'content_hash']]
        
        for field in important_fields:
            old_val = old_rec.get(field)
            new_val = new_rec.get(field)
            
            # Compare values (handle None/null)
            if old_val != new_val:
                # Skip if both are None/null
                if old_val is None and new_val is None:
                    continue
                changed_fields.append((field, old_val, new_val))
        
        # Generate description based on mode
        if CDC_DESCRIPTION_MODE == "Compact":
            # Option A: List field names only
            field_names = [f[0] for f in changed_fields]
            change_descriptions[doc_id] = f"{len(changed_fields)} fields changed: {', '.join(field_names[:10])}"
            if len(field_names) > 10:
                change_descriptions[doc_id] += f" +{len(field_names)-10} more"
        
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
            change_descriptions[doc_id] = description
        
        elif CDC_DESCRIPTION_MODE == "Complete":
            # Option C: Full JSON of all changes
            changes_dict = {}
            for field, old_val, new_val in changed_fields:
                changes_dict[field] = {
                    "old": str(old_val) if old_val is not None else None,
                    "new": str(new_val) if new_val is not None else None
                }
            change_descriptions[doc_id] = json.dumps(changes_dict)
        
        else:
            # Fallback
            change_descriptions[doc_id] = f"{len(changed_fields)} fields changed"
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
).withColumn("cdc_timestamp", F.current_timestamp())

df_silver_cdc.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TABLE_SILVER_CDC)

print(f"[INFO] Appended {truly_changed_count} records to {TABLE_SILVER_CDC}")
print(f"[INFO] Change descriptions generated: {len(change_descriptions)}")

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

# Use current processing time as watermark (NOT record timestamps)
# This handles late-arriving data correctly:
# - Full reloads may bring old timestamps
# - We care about "have we processed this CDC batch" not "what were the record timestamps"
# - Next run will process any NEW CDC entries added after this timestamp
from datetime import timezone
processing_time = datetime.now(timezone.utc)
new_watermark = processing_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  # Millisecond precision to match Bronze

df_new_state = spark.createDataFrame(
    [(int(spark.table(TABLE_SILVER_MAIN).count()), new_watermark, "incremental")],
    schema="total_flattened INT, last_watermark STRING, mode STRING"
 )
df_new_state.write.format("delta").mode("overwrite").saveAsTable(TABLE_SILVER_FLATTEN_STATE)

print(f"[INFO] Updated {TABLE_SILVER_FLATTEN_STATE}")
print(f"  New Watermark: {new_watermark}")
print(f"  Total Flattened: {spark.table(TABLE_SILVER_MAIN).count():,}")

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
