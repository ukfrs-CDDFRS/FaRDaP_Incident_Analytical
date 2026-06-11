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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
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
DRY_RUN = False

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

# Array tables impact (dynamically discover)
print("\n--- SILVER ARRAY TABLES ---")

# Discover all Silver array tables dynamically
all_tables = spark.catalog.listTables()
core_silver_tables = ["fardap_silver_incidents", "fardap_silver_content_hash", 
                      "fardap_silver_flatten_state", "fardap_silver_cdc_log"]
array_tables = [t.name for t in all_tables 
                if t.name.startswith("fardap_silver_") 
                and t.name not in core_silver_tables]

if len(array_tables) == 0:
    print("  No array tables found (they may not have been created yet)")
else:
    print(f"  Found {len(array_tables)} array tables:")
    for table in sorted(array_tables):
        print(f"    - {table}")

array_total = 0
if affected_count > 0 and len(array_tables) > 0:
    print(f"\n  Impact on array tables:")
    for table_name in sorted(array_tables):
        count = spark.table(table_name).filter(
            F.col("documentId").isin(affected_doc_ids)
        ).count()
        array_total += count
        if count > 0:
            print(f"    {table_name}: {count:,} records to delete")
else:
    if affected_count == 0:
        print(f"  All array tables: 0 records (no affected documentIds)")
    elif len(array_tables) == 0:
        print(f"  No array tables to clean up")

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
        # Create temp view with affected documentIds
        df_to_delete = spark.createDataFrame(
            [(doc_id,) for doc_id in affected_doc_ids],
            ["documentId"]
        )
        df_to_delete.createOrReplaceTempView("temp_docs_to_delete")
        
        # Delete Silver incidents using MERGE
        spark.sql("""
            MERGE INTO fardap_silver_incidents AS target
            USING temp_docs_to_delete AS source
            ON target.documentId = source.documentId
            WHEN MATCHED THEN DELETE
        """)
        print(f"✓ Deleted from fardap_silver_incidents")
        
        # Delete Silver content hash
        spark.sql("""
            MERGE INTO fardap_silver_content_hash AS target
            USING temp_docs_to_delete AS source
            ON target.documentId = source.documentId
            WHEN MATCHED THEN DELETE
        """)
        print(f"✓ Deleted from fardap_silver_content_hash")
    
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
    # ARRAY TABLES CLEANUP (Dynamically discovered)
    # ========================================================================
    print("\n[ARRAY TABLES CLEANUP]")
    
    if len(array_tables) > 0:
        if affected_count > 0:
            # Use the temp view we created earlier with known documentIds
            for table_name in sorted(array_tables):
                spark.sql(f"""
                    MERGE INTO {table_name} AS target
                    USING temp_docs_to_delete AS source
                    ON target.documentId = source.documentId
                    WHEN MATCHED THEN DELETE
                """)
                print(f"✓ Deleted from {table_name}")
        else:
            # Fallback: Find and delete orphaned records 
            # (documentIds in array but not in main Silver)
            print("  Finding orphaned array records...")
            
            # Get valid documentIds from Silver main
            df_valid_ids = spark.table("fardap_silver_incidents").select("documentId").distinct()
            df_valid_ids.createOrReplaceTempView("temp_valid_ids")
            
            for table_name in sorted(array_tables):
                # Use LEFT ANTI JOIN to find orphans
                df_orphaned = spark.table(table_name).join(
                    df_valid_ids,
                    on="documentId",
                    how="left_anti"
                ).select("documentId").distinct()
                
                orphaned_count = df_orphaned.count()
                if orphaned_count > 0:
                    df_orphaned.createOrReplaceTempView("temp_orphaned_ids")
                    spark.sql(f"""
                        MERGE INTO {table_name} AS target
                        USING temp_orphaned_ids AS source
                        ON target.documentId = source.documentId
                        WHEN MATCHED THEN DELETE
                    """)
                    print(f"✓ Deleted {orphaned_count} orphaned records from {table_name}")
            
            spark.catalog.dropTempView("temp_valid_ids")
            if spark.catalog._jcatalog.tableExists("temp_orphaned_ids"):
                spark.catalog.dropTempView("temp_orphaned_ids")
    else:
        print("  No array tables to clean up")
    
    # Clean up temp view
    spark.catalog.dropTempView("temp_docs_to_delete")
    
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
