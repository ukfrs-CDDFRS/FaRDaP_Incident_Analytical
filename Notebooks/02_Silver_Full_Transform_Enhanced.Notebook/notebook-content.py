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

# # 02 Silver Full Transform Enhanced
# 
# **Purpose**: Transform ALL Bronze raw_json into normalized Silver layer
# - **Dynamically discover ALL arrays** in JSON (no hardcoded list)
# - Recursively flatten ALL nested structures to columns
# - Each array row gets auto_id (PK) + documentId (FK)
# - Track content_hash to prevent re-flattening unchanged JSON in incremental mode
# - Auto-adapt if new fields/arrays added to API response
# 
# **Output Tables**:
# - `fardap_silver_incidents` (main table with all flattened columns + raw_json)
# - `fardap_silver_<array_name>` (one table per discovered array type)
# - `fardap_silver_content_hash` (tracks which content_hash already flattened)
# - `fardap_silver_flatten_state` (watermark for incremental mode)
# - `fardap_silver_cdc_log` (change tracking)

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
TABLE_SILVER_MAIN = "fardap_silver_incidents"
TABLE_SILVER_FLATTEN_STATE = "fardap_silver_flatten_state"
TABLE_SILVER_CONTENT_HASH = "fardap_silver_content_hash"  # NEW: Track content_hash to prevent re-flattening
TABLE_CDC = "fardap_silver_cdc_log"

# Array tables will be DISCOVERED dynamically (not hardcoded)
ARRAY_TABLES = {}  # Populated in next cell

print(f"[INFO] Configuration:")
print(f"  Lakehouse: {LAKEHOUSE_NAME}")
print(f"  Bronze Input: {TABLE_BRONZE}")
print(f"  Silver Output: {TABLE_SILVER_MAIN}")
print(f"  Array discovery: DYNAMIC (no hardcoded list)")
print(f"  CDC Description Mode: {CDC_DESCRIPTION_MODE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 1: Helper Functions - Dynamic Array Discovery + DEEP Recursive Flattening
# ============================================================================

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
    Returns: dict of {array_name: {table_name, fields, path}}
    
    ENHANCED: Now recursively discovers ALL nested fields within array items,
    not just top-level fields. This captures rescueDetails.by, rescueDetails.method.value, etc.
    """
    arrays = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, list) and len(v) > 0:
                # Found an array - recursively extract ALL fields from sample items
                sample_item = v[0] if len(v) > 0 else {}
                if isinstance(sample_item, dict):
                    # DEEP discovery: recursively find ALL nested fields
                    fields = discover_fields_in_array_item(sample_item)
                    
                    arrays[k] = {
                        "table_name": f"fardap_silver_{k.lower()}",
                        "fields": fields,
                        "path": new_key
                    }
            
            # Recurse into nested structures to find more arrays
            if isinstance(v, dict):
                nested_arrays = discover_arrays_in_json(v, new_key, sep=sep)
                arrays.update(nested_arrays)
            elif isinstance(v, list) and len(v) > 0:
                # Also look for arrays INSIDE array items (nested arrays)
                for item in v[:1]:  # Just check first item
                    if isinstance(item, dict):
                        nested_arrays = discover_arrays_in_json(item, new_key, sep=sep)
                        arrays.update(nested_arrays)
    
    return arrays

def flatten_json(obj, parent_key='', sep='_'):
    """
    Recursively flatten nested JSON object to key-value pairs.
    Skips arrays (they'll be handled separately).
    """
    items = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, list):
                items.append((new_key, None))  # Skip arrays
            elif isinstance(v, dict):
                items.extend(flatten_json(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
    
    return dict(items)

def extract_arrays_from_json(json_str, document_id, array_configs):
    """
    Extract all array items from JSON using discovered array configs.
    Returns dict: {table_name: [(row_dict), ...]}
    
    ENHANCED: Now recursively flattens ALL nested structures within each array item.
    Fields like rescueDetails.by become rescueDetails_by columns.
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

print("[INFO] Helper functions loaded:")
print("  - discover_arrays_in_json: Dynamically finds ALL arrays")
print("  - discover_fields_in_array_item: DEEP discovery of nested fields")
print("  - flatten_nested_item: DEEP flattening of array items")
print("  - flatten_json: Recursively flattens nested structures")
print("  - extract_arrays_from_json: Extracts array items with FULL nesting")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 2: Read Bronze + Dynamically Discover ALL Arrays
# ============================================================================

df_bronze = spark.table(TABLE_BRONZE).select("documentId", "raw_json", "sync_timestamp", "content_hash")

bronze_count = df_bronze.count()
print(f"[INFO] Loaded {bronze_count:,} records from {TABLE_BRONZE}")

# Scan ALL records to discover ALL arrays (ensures 100% coverage of rare arrays)
print(f"[INFO] Scanning ALL {bronze_count:,} records to discover array structures...")
print(f"[INFO] This may take a few minutes but ensures complete array discovery...")
sample_jsons = df_bronze.select("raw_json").collect()

# Discover arrays from all samples
arrays_discovered_count = 0
for idx, sample_row in enumerate(sample_jsons):
    try:
        sample_content = json.loads(sample_row[0])
        discovered = discover_arrays_in_json(sample_content)
        
        # Merge with existing ARRAY_TABLES (accumulate all discovered arrays)
        for array_name, array_config in discovered.items():
            if array_name not in ARRAY_TABLES:
                ARRAY_TABLES[array_name] = array_config
                arrays_discovered_count += 1
                print(f"  [DISCOVERED] {array_name} (record {idx+1}/{len(sample_jsons)})")
            else:
                # Merge fields (in case different records have different fields)
                existing_fields = set(ARRAY_TABLES[array_name]['fields'])
                new_fields = set(array_config['fields'])
                merged_fields = list(existing_fields | new_fields)
                if len(merged_fields) > len(existing_fields):
                    print(f"  [EXTENDED] {array_name} +{len(merged_fields) - len(existing_fields)} fields (record {idx+1}/{len(sample_jsons)})")
                ARRAY_TABLES[array_name]['fields'] = merged_fields
    except Exception as e:
        continue

print(f"\n[INFO] ✅ Discovered {len(ARRAY_TABLES)} array types dynamically:")
for array_name, config in sorted(ARRAY_TABLES.items()):
    print(f"  - {array_name:30s} → {config['table_name']:50s} ({len(config['fields'])} fields)")

print(f"\n[INFO] If API adds new array fields in future, they'll be auto-discovered here")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 3: Flatten All JSON to Columns
# ============================================================================

flatten_udf = F.udf(
    lambda json_str: flatten_json(json.loads(json_str)) if json_str else {}, 
    T.MapType(T.StringType(), T.StringType())
)

df_flattened = df_bronze.withColumn("_flat", flatten_udf(F.col("raw_json")))

# Get all possible keys from all records (handles schema variance)
all_keys = df_flattened.select(F.explode(F.map_keys(F.col("_flat")))).distinct().collect()
all_keys = sorted([row[0] for row in all_keys if row[0] is not None])

print(f"[INFO] Discovered {len(all_keys)} unique flattened fields across all records")
print(f"[INFO] First 20 fields: {all_keys[:20]}")

# Convert map to individual columns
for key in all_keys:
    col_name = key.lower().replace(" ", "_")
    df_flattened = df_flattened.withColumn(
        col_name,
        F.col("_flat")[key].cast(T.StringType())
    )

df_flattened = df_flattened.drop("_flat")

# Add metadata columns
df_silver = df_flattened.withColumn(
    "flattened_timestamp", F.current_timestamp()
).withColumn(
    "processed_at", F.current_timestamp()
)

print(f"[INFO] Flattened schema has {len(df_silver.columns)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 4: Extract Arrays into Normalized Tables (Auto-ID + DocumentId FK)
# ============================================================================

extracted_arrays = {}
bronze_data = df_bronze.collect()

print(f"[INFO] Extracting arrays from {len(bronze_data)} records...")

for row in bronze_data:
    doc_id = row.documentId
    raw_json = row.raw_json
    
    arrays_dict = extract_arrays_from_json(raw_json, doc_id, ARRAY_TABLES)
    
    for table_name, array_data in arrays_dict.items():
        if table_name not in extracted_arrays:
            extracted_arrays[table_name] = []
        extracted_arrays[table_name].extend(array_data)

print(f"[INFO] Writing array tables:")

total_array_rows = 0
for table_name, records in extracted_arrays.items():
    if records:
        # STEP 1: Collect ALL unique keys across ALL records for this table
        all_keys = set()
        for record in records:
            all_keys.update(record.keys())
        all_keys = sorted(all_keys)
        
        # STEP 2: Normalize all records - ensure every record has ALL keys (None for missing)
        # AND convert all non-metadata values to strings to prevent type conflicts
        metadata_cols = ["documentId", "array_index"]
        normalized_records = []
        for record in records:
            normalized = {}
            for key in all_keys:
                val = record.get(key)
                if key in metadata_cols:
                    normalized[key] = val  # Keep original type for metadata
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
        
        # Add auto-incrementing ID
        df_array = df_array.withColumn(
            "array_id",
            F.row_number().over(Window.partitionBy().orderBy("documentId", "array_index"))
        )
        df_array = df_array.withColumn("processed_at", F.current_timestamp())
        
        # Use overwriteSchema=true for full load to completely replace existing schema
        # This avoids conflicts with existing tables that have different column types
        df_array.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        
        row_count = len(records)
        total_array_rows += row_count
        print(f"  ✓ {table_name}: {row_count:,} rows, {len(df_array.columns)} columns")

print(f"[INFO] Total array rows extracted: {total_array_rows:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 5: Write Flattened Incidents to Silver Main Table
# ============================================================================

# Reorder columns: documentId first, then metadata, then flattened columns
cols_order = ["documentId", "raw_json", "content_hash", "sync_timestamp", "flattened_timestamp", "processed_at"]
other_cols = [c for c in df_silver.columns if c not in cols_order]
df_silver = df_silver.select(cols_order + sorted(other_cols))

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(TABLE_SILVER_MAIN)

silver_count = df_silver.count()
print(f"[INFO] Wrote {silver_count:,} records to {TABLE_SILVER_MAIN}")
print(f"[INFO] Silver table schema: {len(df_silver.columns)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 6: Track Flattening State + Content Hashes
# ============================================================================

# Create watermark state table
df_state = spark.createDataFrame(
    [(int(df_silver.count()), datetime.now().isoformat(), "full_load")],
    schema="total_flattened INT, last_watermark STRING, mode STRING"
)
df_state.write.format("delta").mode("overwrite").saveAsTable(TABLE_SILVER_FLATTEN_STATE)

print(f"[INFO] Created {TABLE_SILVER_FLATTEN_STATE}")
print(f"  Total Flattened: {int(df_silver.count()):,}")
print(f"  Watermark: {datetime.now().isoformat()}")

# Create content_hash tracking table: prevents re-flattening unchanged JSON
df_content_hash = df_bronze.select(
    F.col("documentId"),
    F.col("content_hash"),
    F.current_timestamp().alias("last_flattened_at")
)
df_content_hash.write.format("delta").mode("overwrite").saveAsTable(TABLE_SILVER_CONTENT_HASH)

print(f"\n[INFO] Created {TABLE_SILVER_CONTENT_HASH}")
print(f"  Tracking content_hash for {df_content_hash.count():,} records")
print(f"  ✅ Incremental mode will ONLY re-flatten if content_hash changes")
print(f"  ✅ This prevents wasted flattening of unchanged JSON")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 7: Create CDC Log with Descriptions
# ============================================================================

print(f"\n[INFO] Generating CDC log (Mode: {CDC_DESCRIPTION_MODE})...")

# For full load, all records are INSERTs
# Generate descriptions based on record content
records_sample = df_silver.select("documentId").limit(100).collect()
print(f"  All {df_silver.count():,} records are INSERTs (full load)")

# For inserts, count non-null fields
if CDC_DESCRIPTION_MODE == "Compact":
    # Simple: just say it's a new record
    df_cdc = df_silver.select(
        F.col("documentId"),
        F.lit("insert").alias("op_type"),
        F.col("flattened_timestamp").alias("flattened_at"),
        F.current_timestamp().alias("cdc_timestamp"),
        F.lit("New record created").alias("change_description")
    )

elif CDC_DESCRIPTION_MODE == "Detailed":
    # Count non-null fields per record
    # Create an expression that counts non-null columns
    content_cols = [col for col in df_silver.columns if col.startswith('content_')]
    non_null_expr = sum([F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in content_cols])
    
    df_cdc = df_silver.select(
        F.col("documentId"),
        F.lit("insert").alias("op_type"),
        F.col("flattened_timestamp").alias("flattened_at"),
        F.current_timestamp().alias("cdc_timestamp"),
        F.concat(
            F.lit("New record with "),
            non_null_expr.cast(T.StringType()),
            F.lit(" populated fields")
        ).alias("change_description")
    )

elif CDC_DESCRIPTION_MODE == "Complete":
    # For full load, create JSON showing all populated fields
    # This is expensive, so we'll create a simplified version
    content_cols = [col for col in df_silver.columns if col.startswith('content_')]
    non_null_expr = sum([F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in content_cols])
    
    df_cdc = df_silver.select(
        F.col("documentId"),
        F.lit("insert").alias("op_type"),
        F.col("flattened_timestamp").alias("flattened_at"),
        F.current_timestamp().alias("cdc_timestamp"),
        F.concat(
            F.lit('{"operation": "insert", "field_count": '),
            non_null_expr.cast(T.StringType()),
            F.lit(', "timestamp": "'),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.lit('"}')
        ).alias("change_description")
    )
else:
    # Fallback
    df_cdc = df_silver.select(
        F.col("documentId"),
        F.lit("insert").alias("op_type"),
        F.col("flattened_timestamp").alias("flattened_at"),
        F.current_timestamp().alias("cdc_timestamp"),
        F.lit("New record").alias("change_description")
    )

df_cdc.write.format("delta").mode("overwrite").saveAsTable(TABLE_CDC)

print(f"[INFO] Created CDC log: {TABLE_CDC}")
print(f"[INFO] CDC records: {df_cdc.count():,} (all 'insert' for full load)")
print(f"[INFO] Change descriptions: {CDC_DESCRIPTION_MODE} mode")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 8: Validation & Summary
# ============================================================================

df_validate_main = spark.table(TABLE_SILVER_MAIN)
print("[VALIDATION] Silver Main Table:")
print(f"  Records: {df_validate_main.count():,}")
print(f"  Columns: {len(df_validate_main.columns)}")

print("\n[VALIDATION] Array Tables:")
for array_type, table_config in sorted(ARRAY_TABLES.items()):
    table_name = table_config["table_name"]
    try:
        df_arr = spark.table(table_name)
        arr_count = df_arr.count()
        if arr_count > 0:
            print(f"  ✓ {table_name}: {arr_count:,} rows")
    except Exception as e:
        print(f"  - {table_name}: Not created (no data)")

print(f"\n[SUCCESS] ✅ Silver Layer Full Transform Complete!")
print(f"  Main Table: {TABLE_SILVER_MAIN} ({df_validate_main.count():,} rows)")
print(f"  Array Tables: {len([t for t in ARRAY_TABLES.keys() if spark.table(ARRAY_TABLES[t]['table_name']).count() > 0])} created")
print(f"  Content Hash Tracking: Enabled")
print(f"  Schema Flexibility: Fully Dynamic")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
