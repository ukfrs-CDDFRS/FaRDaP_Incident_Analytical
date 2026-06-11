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

# # Wipe Silver Tables Only
# 
# **Purpose**: Drop all `fardap_silver_*` tables to prepare for re-running the silver transformation
# 
# **Safety**: Only drops silver tables - leaves bronze tables intact
# 
# **Tables Dropped**:
# - `fardap_silver_incidents`
# - `fardap_silver_*` (all array tables)
# - `fardap_silver_content_hash`
# - `fardap_silver_flatten_state`
# - `fardap_silver_cdc_log`

# CELL ********************

# ============================================================================
# Drop All Silver Tables
# ============================================================================

print("[INFO] Finding all silver tables to drop...")

# Get all tables in the current database
all_tables = spark.sql("SHOW TABLES").collect()

# Filter for fardap_silver_* tables
silver_tables = [row.tableName for row in all_tables if row.tableName.startswith('fardap_silver_')]

print(f"[INFO] Found {len(silver_tables)} silver tables to drop:")
for table in sorted(silver_tables):
    print(f"  - {table}")

# Drop each silver table
print(f"\n[INFO] Dropping silver tables...")
dropped_count = 0
for table in silver_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"  ✓ Dropped: {table}")
        dropped_count += 1
    except Exception as e:
        print(f"  ✗ Failed to drop {table}: {str(e)}")

print(f"\n[SUCCESS] ✅ Dropped {dropped_count}/{len(silver_tables)} silver tables")
print(f"[INFO] Bronze tables remain intact")
print(f"[INFO] Ready to re-run 02_Silver_Full_Transform_Enhanced notebook")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Verify Bronze Tables Still Exist
# ============================================================================

print("[VERIFICATION] Checking bronze tables are still present...")

bronze_tables = ['fardap_bronze_incidents', 'fardap_sync_state']

for table in bronze_tables:
    try:
        count = spark.table(table).count()
        print(f"  ✓ {table}: {count:,} records")
    except Exception as e:
        print(f"  ✗ {table}: NOT FOUND (ERROR: {str(e)})")

print("\n[INFO] Bronze layer is intact - ready for silver transformation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
