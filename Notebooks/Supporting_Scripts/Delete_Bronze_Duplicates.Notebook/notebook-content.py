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
# META       "default_lakehouse_workspace_id": "04c5b96c-21ba-4ebb-812e-bed01bbac715"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Delete Bronze Duplicate Records
# 
# **Purpose**: Delete duplicate records from fardap_bronze_incidents
# - Identifies all documentIds that appear more than once
# - For each duplicate, keeps the most recent record (by sync_timestamp)
# - Deletes the older duplicate(s)
# 
# **Root Cause**: Bronze duplicates are causing Silver duplicates

# CELL ********************

# ============================================================================
# STEP 1: Identify Duplicates in Bronze
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 80)
print("PHASE 1: IDENTIFYING BRONZE DUPLICATES")
print("=" * 80)
print()

# Find all documentIds that have duplicates
duplicates_df = spark.sql("""
    SELECT 
        documentId,
        COUNT(*) as duplicate_count,
        MIN(sync_timestamp) as earliest_sync,
        MAX(sync_timestamp) as latest_sync
    FROM inc_fardap_lakehouse.fardap_bronze_incidents
    GROUP BY documentId
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC, documentId
""")

print("--- Duplicate DocumentIds in fardap_bronze_incidents ---")
duplicates_df.show(100, truncate=False)

duplicate_count = duplicates_df.count()
total_duplicate_rows = duplicates_df.agg(F.sum("duplicate_count")).collect()[0][0]

print(f"\nTotal documentIds with duplicates: {duplicate_count}")
print(f"Total duplicate rows: {total_duplicate_rows}")
print(f"Rows to keep: {duplicate_count}")
print(f"Rows to delete: {total_duplicate_rows - duplicate_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 2: Sample Duplicate Records - Verify They Are Identical
# ============================================================================

print("\n" + "=" * 80)
print("PHASE 2: SAMPLE DUPLICATE RECORDS")
print("=" * 80)
print()

# Get a sample duplicate documentId to examine
sample_dup_id = duplicates_df.first().documentId

print(f"Sample duplicate documentId: {sample_dup_id}")
print()

# Show all records for this documentId
sample_records = spark.sql(f"""
    SELECT 
        documentId,
        sync_timestamp,
        change_ts,
        op_type,
        content_hash
    FROM inc_fardap_lakehouse.fardap_bronze_incidents
    WHERE documentId = '{sample_dup_id}'
    ORDER BY sync_timestamp DESC
""")

print("All records for this documentId:")
sample_records.show(truncate=False)

# Check if content_hash is the same (indicating identical data)
hash_check = spark.sql(f"""
    SELECT 
        documentId,
        COUNT(DISTINCT content_hash) as unique_hashes,
        COUNT(*) as total_rows
    FROM inc_fardap_lakehouse.fardap_bronze_incidents
    WHERE documentId IN (
        SELECT documentId
        FROM inc_fardap_lakehouse.fardap_bronze_incidents
        GROUP BY documentId
        HAVING COUNT(*) > 1
    )
    GROUP BY documentId
    HAVING COUNT(DISTINCT content_hash) > 1
""")

different_content_count = hash_check.count()
print(f"\nDocumentIds where duplicates have DIFFERENT content_hash: {different_content_count}")
if different_content_count > 0:
    print("⚠️  Warning: Some duplicates have different content!")
    hash_check.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 3: Identify Records to Keep vs Delete
# ============================================================================

print("\n" + "=" * 80)
print("PHASE 3: IDENTIFYING RECORDS TO DELETE")
print("=" * 80)
print()

# Strategy: For each duplicate documentId, keep the record with the MOST RECENT sync_timestamp
# Delete all older records

# Get all bronze records and rank them by sync_timestamp within each documentId
bronze_df = spark.table("inc_fardap_lakehouse.fardap_bronze_incidents")

# Add row number partitioned by documentId, ordered by sync_timestamp DESC
window_spec = Window.partitionBy("documentId").orderBy(F.col("sync_timestamp").desc())
ranked_df = bronze_df.withColumn("row_num", F.row_number().over(window_spec))

# Filter to duplicates only (documentIds with multiple records)
duplicates_only = ranked_df.join(
    duplicates_df.select("documentId"),
    on="documentId",
    how="inner"
)

print("--- Records to DELETE (row_num > 1 = older duplicates) ---")
records_to_delete = duplicates_only.filter(F.col("row_num") > 1).select(
    "documentId", "sync_timestamp", "change_ts", "op_type", "row_num"
)
records_to_delete.orderBy("documentId", "row_num").show(50, truncate=False)

delete_count = records_to_delete.count()
print(f"\nTotal records to delete: {delete_count}")

print("\n--- Records to KEEP (row_num = 1 = most recent) ---")
records_to_keep = duplicates_only.filter(F.col("row_num") == 1).select(
    "documentId", "sync_timestamp", "change_ts", "op_type", "row_num"
)
records_to_keep.orderBy("documentId").show(50, truncate=False)

keep_count = records_to_keep.count()
print(f"\nTotal records to keep: {keep_count}")

# Create temp view for deletion
records_to_delete.select("documentId", "sync_timestamp").createOrReplaceTempView("temp_records_to_delete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 4: DELETE DUPLICATES (UNCOMMENT TO EXECUTE)
# ============================================================================

print("\n" + "=" * 80)
print("PHASE 4: DELETING DUPLICATE RECORDS FROM BRONZE")
print("=" * 80)
print()

# IMPORTANT: Review the records above before uncommenting this section!
# Uncomment the deletion code below to proceed:

"""
print(f"Deleting {delete_count} duplicate records from fardap_bronze_incidents...")
print("Strategy: Keep most recent record per documentId (highest sync_timestamp)")
print()

# Get list of records to delete
delete_list = records_to_delete.select("documentId", "sync_timestamp").collect()

# Delete in batches to avoid query size limits
BATCH_SIZE = 100
batches = [delete_list[i:i + BATCH_SIZE] for i in range(0, len(delete_list), BATCH_SIZE)]

print(f"Processing {len(delete_list)} deletions in {len(batches)} batches...")

try:
    deleted_total = 0
    for batch_idx, batch in enumerate(batches, 1):
        # Build WHERE clause for this batch
        # We match on BOTH documentId AND sync_timestamp to ensure we delete the exact right record
        conditions = []
        for row in batch:
            doc_id = row.documentId
            sync_ts = row.sync_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
            conditions.append(f"(documentId = '{doc_id}' AND sync_timestamp = '{sync_ts}')")
        
        where_clause = " OR ".join(conditions)
        
        # Execute DELETE
        spark.sql(f'''
            DELETE FROM inc_fardap_lakehouse.fardap_bronze_incidents
            WHERE {where_clause}
        ''')
        
        print(f"  Batch {batch_idx}/{len(batches)}: Deleted {len(batch)} records")
        deleted_total += len(batch)
    
    print()
    print("=" * 80)
    print("DELETION COMPLETE")
    print("=" * 80)
    print(f"Total records deleted: {deleted_total}")
    print()
    
except Exception as e:
    print(f"ERROR during deletion: {str(e)}")
    raise
"""

print("\n⚠️  DELETION CODE IS COMMENTED OUT")
print("Review the records above, then uncomment the deletion section to proceed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# VERIFICATION: Check for Remaining Duplicates (Run After Deletion)
# ============================================================================

print("\n" + "=" * 80)
print("VERIFICATION: Checking for Remaining Duplicates in Bronze")
print("=" * 80)
print()

remaining_duplicates = spark.sql("""
    SELECT 
        documentId,
        COUNT(*) as duplicate_count
    FROM inc_fardap_lakehouse.fardap_bronze_incidents
    GROUP BY documentId
    HAVING COUNT(*) > 1
""")

remaining_count = remaining_duplicates.count()

if remaining_count == 0:
    print("✓ SUCCESS: No duplicates found in fardap_bronze_incidents")
    print()
    print("Next steps:")
    print("1. Delete Silver duplicates (run Delete_Silver_Duplicates notebook)")
    print("2. Re-run Silver transformation to recreate records from cleaned Bronze")
else:
    print(f"⚠️  WARNING: {remaining_count} documentIds still have duplicates")
    remaining_duplicates.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
