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

# # Delete Content Hash for Duplicate DocumentIds
# 
# **Purpose**: Remove content_hash entries for duplicate documentIds so Silver will reprocess them
# 
# **Why**: Silver incremental transform skips records if content_hash hasn't changed.
# Since we deleted Bronze duplicates but kept the data, the content_hash is the same,
# so Silver thinks "already processed, skip". We need to delete the hash entries to force reprocessing.

# CELL ********************

# ============================================================================
# STEP 1: Identify DocumentIds to Clean
# ============================================================================

from pyspark.sql import functions as F

print("=" * 80)
print("PHASE 1: IDENTIFYING CONTENT HASH ENTRIES TO DELETE")
print("=" * 80)
print()

# Find documentIds that recently appeared in Bronze (after watermark)
# These are the ones we need to reprocess
recent_bronze = spark.sql("""
    SELECT DISTINCT documentId
    FROM inc_fardap_lakehouse.fardap_bronze_incidents
    WHERE sync_timestamp >= '2026-06-18T14:31:33.356Z'
""")

print(f"DocumentIds in Bronze after watermark: {recent_bronze.count()}")
recent_bronze.show(20, truncate=False)

# Check which of these have entries in content_hash table
hash_entries = spark.sql("""
    SELECT DISTINCT documentId
    FROM inc_fardap_lakehouse.fardap_silver_content_hash
    WHERE documentId IN (
        SELECT DISTINCT documentId
        FROM inc_fardap_lakehouse.fardap_bronze_incidents
        WHERE sync_timestamp >= '2026-06-18T14:31:33.356Z'
    )
""")

hash_count = hash_entries.count()
print(f"\nDocumentIds with content_hash entries: {hash_count}")
hash_entries.show(20, truncate=False)

# Create temp view for deletion
hash_entries.createOrReplaceTempView("temp_hashes_to_delete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 2: DELETE Content Hash Entries (UNCOMMENT TO EXECUTE)
# ============================================================================

print("\n" + "=" * 80)
print("PHASE 2: DELETING CONTENT HASH ENTRIES")
print("=" * 80)
print()

# UNCOMMENT THE CODE BELOW TO DELETE:

"""
print(f"Deleting content_hash entries for {hash_count} documentIds...")
print()

# Get list of documentIds to delete
doc_ids_to_delete = [row.documentId for row in hash_entries.collect()]

# Delete in batches
BATCH_SIZE = 100
batches = [doc_ids_to_delete[i:i + BATCH_SIZE] for i in range(0, len(doc_ids_to_delete), BATCH_SIZE)]

print(f"Processing {len(doc_ids_to_delete)} deletions in {len(batches)} batches...")

try:
    deleted_total = 0
    for batch_idx, batch in enumerate(batches, 1):
        # Build WHERE clause for this batch
        doc_ids_str = ",".join([f"'{doc_id}'" for doc_id in batch])
        
        # Execute DELETE
        spark.sql(f'''
            DELETE FROM inc_fardap_lakehouse.fardap_silver_content_hash
            WHERE documentId IN ({doc_ids_str})
        ''')
        
        print(f"  Batch {batch_idx}/{len(batches)}: Deleted {len(batch)} entries")
        deleted_total += len(batch)
    
    print()
    print("=" * 80)
    print("DELETION COMPLETE")
    print("=" * 80)
    print(f"Total content_hash entries deleted: {deleted_total}")
    print()
    print("Next step: Run Silver incremental transform notebook")
    print("Silver will now reprocess these {deleted_total} documentIds from Bronze")
    print()
    
except Exception as e:
    print(f"ERROR during deletion: {str(e)}")
    raise
"""

print("\n⚠️  DELETION CODE IS COMMENTED OUT")
print("Review the documentIds above, then uncomment the deletion section to proceed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# VERIFICATION: Check Remaining Content Hash Entries
# ============================================================================

print("\n" + "=" * 80)
print("VERIFICATION: Checking Remaining Content Hash Entries")
print("=" * 80)
print()

remaining_hashes = spark.sql("""
    SELECT COUNT(*) as remaining_count
    FROM inc_fardap_lakehouse.fardap_silver_content_hash
    WHERE documentId IN (
        SELECT DISTINCT documentId
        FROM inc_fardap_lakehouse.fardap_bronze_incidents
        WHERE sync_timestamp >= '2026-06-18T14:31:33.356Z'
    )
""")

remaining_count = remaining_hashes.first().remaining_count

if remaining_count == 0:
    print("✓ SUCCESS: All content_hash entries deleted for recent Bronze records")
    print()
    print("Silver incremental transform will now reprocess these documentIds")
else:
    print(f"⚠️  WARNING: {remaining_count} content_hash entries still exist")

# Show total content_hash entries remaining
total_hashes = spark.sql("SELECT COUNT(*) as cnt FROM inc_fardap_lakehouse.fardap_silver_content_hash").first().cnt
print(f"\nTotal content_hash entries in table: {total_hashes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
