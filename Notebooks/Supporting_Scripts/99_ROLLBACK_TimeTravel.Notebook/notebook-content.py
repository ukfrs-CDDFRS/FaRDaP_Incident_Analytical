# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ============================================================================
# TIME-TRAVEL ROLLBACK NOTEBOOK
# ============================================================================
# Completely revert Bronze, Silver, and all array tables to a specific point
# in time - as if no processing happened after the cutoff timestamp.
#
# USAGE:
#   1. Set ROLLBACK_TIMESTAMP below (the point you want to revert TO)
#   2. Set DRY_RUN = True to preview impact
#   3. Set DRY_RUN = False to execute rollback
# ============================================================================

from pyspark.sql import functions as F
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# Set your rollback cutoff timestamp (everything after this will be erased)
ROLLBACK_TIMESTAMP = "2026-06-11T00:00:00Z"  # ISO 8601 format

# DRY RUN mode: Set to True to preview, False to execute
DRY_RUN = True

print("="*80)
print("TIME-TRAVEL ROLLBACK")
print("="*80)
print(f"Rollback Cutoff: {ROLLBACK_TIMESTAMP}")
print(f"Mode: {'DRY RUN (preview only)' if DRY_RUN else 'EXECUTE (will delete data!)'}")
print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 1: Identify affected documentIds
# ============================================================================

print("\n[STEP 1] Identifying affected documentIds...")

# Get all documentIds that were processed after cutoff
df_affected = spark.table("fardap_bronze_cdc_log").filter(
    F.col("sync_timestamp") > F.lit(ROLLBACK_TIMESTAMP)
)

affected_doc_ids = [row.documentId for row in df_affected.select("documentId").distinct().collect()]
affected_count = len(affected_doc_ids)

print(f"✓ Found {affected_count} unique documentIds affected by runs after cutoff")

if affected_count > 0 and affected_count <= 10:
    print(f"  DocumentIds: {affected_doc_ids}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 2: Preview impact on all tables
# ============================================================================

print("\n[STEP 2] Analyzing impact on all tables...\n")

# Bronze layer impact
print("--- BRONZE LAYER ---")

bronze_incidents_to_delete = spark.table("fardap_bronze_incidents").filter(
    F.col("sync_timestamp") > F.lit(ROLLBACK_TIMESTAMP)
).count()
print(f"  fardap_bronze_incidents: {bronze_incidents_to_delete:,} records to delete")

bronze_cdc_to_delete = spark.table("fardap_bronze_cdc_log").filter(
    F.col("sync_timestamp") > F.lit(ROLLBACK_TIMESTAMP)
)
bronze_cdc_count = bronze_cdc_to_delete.count()
bronze_inserts = bronze_cdc_to_delete.filter(F.col("op_type") == "insert").count()
bronze_updates = bronze_cdc_to_delete.filter(F.col("op_type") == "update").count()
print(f"  fardap_bronze_cdc_log: {bronze_cdc_count:,} records ({bronze_inserts} inserts, {bronze_updates} updates)")

current_bronze_watermark = spark.table("fardap_sync_state").select("last_watermark").collect()[0][0]
print(f"  fardap_sync_state: watermark will revert from '{current_bronze_watermark}' to '{ROLLBACK_TIMESTAMP}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver layer impact
print("\n--- SILVER LAYER ---")

if affected_count > 0:
    silver_incidents_to_delete = spark.table("fardap_silver_incidents").filter(
        F.col("documentId").isin(affected_doc_ids)
    ).count()
    print(f"  fardap_silver_incidents: {silver_incidents_to_delete:,} records to delete")
    
    silver_hash_to_delete = spark.table("fardap_silver_content_hash").filter(
        F.col("documentId").isin(affected_doc_ids)
    ).count()
    print(f"  fardap_silver_content_hash: {silver_hash_to_delete:,} records to delete")
else:
    print(f"  fardap_silver_incidents: 0 records (no affected documentIds)")
    print(f"  fardap_silver_content_hash: 0 records (no affected documentIds)")

if spark.catalog.tableExists("fardap_silver_cdc_log"):
    silver_cdc_to_delete = spark.table("fardap_silver_cdc_log").filter(
        F.col("cdc_timestamp") > F.lit(ROLLBACK_TIMESTAMP)
    ).count()
    print(f"  fardap_silver_cdc_log: {silver_cdc_to_delete:,} records to delete")
else:
    print(f"  fardap_silver_cdc_log: table doesn't exist (skip)")

if spark.catalog.tableExists("fardap_silver_flatten_state"):
    current_silver_watermark = spark.table("fardap_silver_flatten_state").select("last_watermark").collect()[0][0]
    print(f"  fardap_silver_flatten_state: watermark will revert from '{current_silver_watermark}' to '{ROLLBACK_TIMESTAMP}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Array tables impact
print("\n--- SILVER ARRAY TABLES ---")

array_tables = [
    "fardap_silver_actionstaken",
    "fardap_silver_appliances",
    "fardap_silver_casualties",
    "fardap_silver_closeststation",
    "fardap_silver_falsealarmalertingmethod",
    "fardap_silver_falsealarmreasons",
    "fardap_silver_firerescuedetails",
    "fardap_silver_ignitionsources",
    "fardap_silver_itemsignited",
    "fardap_silver_propertytypes",
    "fardap_silver_ssrsupplied"
]

array_total = 0
if affected_count > 0:
    for table_name in array_tables:
        if spark.catalog.tableExists(table_name):
            count = spark.table(table_name).filter(
                F.col("documentId").isin(affected_doc_ids)
            ).count()
            array_total += count
            if count > 0:
                print(f"  {table_name}: {count:,} records to delete")
        else:
            print(f"  {table_name}: table doesn't exist (skip)")
else:
    print(f"  All array tables: 0 records (no affected documentIds)")

print(f"\n  Total array records: {array_total:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 3: Execute rollback (if DRY_RUN = False)
# ============================================================================

if DRY_RUN:
    print("\n" + "="*80)
    print("DRY RUN COMPLETE - No data was deleted")
    print("="*80)
    print("To execute the rollback:")
    print("  1. Review the impact summary above")
    print("  2. Set DRY_RUN = False in the configuration cell")
    print("  3. Re-run this notebook")
    print("="*80)
    
else:
    print("\n" + "="*80)
    print("EXECUTING ROLLBACK - DELETING DATA")
    print("="*80)
    
    # ========================================================================
    # BRONZE LAYER CLEANUP
    # ========================================================================
    print("\n[BRONZE CLEANUP]")
    
    # Delete Bronze incidents added after cutoff
    spark.sql(f"""
        DELETE FROM fardap_bronze_incidents
        WHERE sync_timestamp > '{ROLLBACK_TIMESTAMP}'
    """)
    print(f"✓ Deleted from fardap_bronze_incidents")
    
    # Delete Bronze CDC log
    spark.sql(f"""
        DELETE FROM fardap_bronze_cdc_log
        WHERE sync_timestamp > '{ROLLBACK_TIMESTAMP}'
    """)
    print(f"✓ Deleted from fardap_bronze_cdc_log")
    
    # Revert Bronze watermark
    spark.sql(f"""
        UPDATE fardap_sync_state
        SET last_watermark = '{ROLLBACK_TIMESTAMP}'
    """)
    print(f"✓ Reverted fardap_sync_state watermark to {ROLLBACK_TIMESTAMP}")
    
    # ========================================================================
    # SILVER LAYER CLEANUP
    # ========================================================================
    print("\n[SILVER CLEANUP]")
    
    if affected_count > 0:
        # Delete Silver incidents for affected documentIds
        spark.table("fardap_silver_incidents").filter(
            F.col("documentId").isin(affected_doc_ids)
        ).write.format("delta").mode("overwrite").saveAsTable("temp_silver_incidents_to_delete")
        
        spark.sql("""
            DELETE FROM fardap_silver_incidents
            WHERE documentId IN (SELECT documentId FROM temp_silver_incidents_to_delete)
        """)
        print(f"✓ Deleted from fardap_silver_incidents")
        
        # Delete Silver content hash
        spark.sql("""
            DELETE FROM fardap_silver_content_hash
            WHERE documentId IN (SELECT documentId FROM temp_silver_incidents_to_delete)
        """)
        print(f"✓ Deleted from fardap_silver_content_hash")
        
        spark.sql("DROP TABLE IF EXISTS temp_silver_incidents_to_delete")
    
    # Delete Silver CDC log
    if spark.catalog.tableExists("fardap_silver_cdc_log"):
        spark.sql(f"""
            DELETE FROM fardap_silver_cdc_log
            WHERE cdc_timestamp > '{ROLLBACK_TIMESTAMP}'
        """)
        print(f"✓ Deleted from fardap_silver_cdc_log")
    
    # Revert Silver flatten state watermark
    if spark.catalog.tableExists("fardap_silver_flatten_state"):
        spark.sql(f"""
            UPDATE fardap_silver_flatten_state
            SET last_watermark = '{ROLLBACK_TIMESTAMP}'
        """)
        print(f"✓ Reverted fardap_silver_flatten_state watermark")
    
    # ========================================================================
    # ARRAY TABLES CLEANUP
    # ========================================================================
    print("\n[ARRAY TABLES CLEANUP]")
    
    if affected_count > 0:
        for table_name in array_tables:
            if spark.catalog.tableExists(table_name):
                spark.sql(f"""
                    DELETE FROM {table_name}
                    WHERE documentId IN (
                        SELECT DISTINCT documentId 
                        FROM fardap_bronze_cdc_log 
                        WHERE sync_timestamp > '{ROLLBACK_TIMESTAMP}'
                    )
                """)
                print(f"✓ Deleted from {table_name}")
    
    print("\n" + "="*80)
    print("ROLLBACK COMPLETE!")
    print("="*80)
    print(f"Pipeline reverted to: {ROLLBACK_TIMESTAMP}")
    print("\nNext steps:")
    print("  1. Run Bronze Incremental Sync (with fixed code)")
    print("  2. Run Silver Incremental Transform (with fixed code)")
    print("  3. Verify CDC logs show correct op_type values")
    print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
