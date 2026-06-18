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

# # Delete Silver Duplicate Records
# 
# **Purpose**: Delete ALL duplicate records from silver tables (~475 duplicates)
# - Identifies all documentIds that have duplicates
# - Deletes ALL records for those documentIds from incidents and all array tables
# - You will recreate them from bronze in the next step
# 
# **Strategy**: Uses Delta Lake DELETE operations (not T-SQL)

# CELL ********************

# ============================================================================
# STEP 1: Identify Duplicate DocumentIds
# ============================================================================

from pyspark.sql import functions as F

print("=" * 80)
print("PHASE 1: IDENTIFYING DUPLICATES")
print("=" * 80)
print()

# Find all documentIds that have duplicates in the incidents table
duplicates_df = spark.sql("""
    SELECT 
        documentId,
        COUNT(*) as duplicate_count
    FROM inc_fardap_lakehouse.fardap_silver_incidents
    GROUP BY documentId
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC, documentId
""")

print("--- Duplicate DocumentIds in fardap_silver_incidents ---")
duplicates_df.show(100, truncate=False)

duplicate_count = duplicates_df.count()
print(f"\nTotal documentIds with duplicates: {duplicate_count}")

# Get list of duplicate documentIds for deletion
duplicate_doc_ids = [row.documentId for row in duplicates_df.collect()]
print(f"\nSample duplicate documentIds: {duplicate_doc_ids[:10]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 2: Count Records to be Deleted from Each Table
# ============================================================================

print("\n" + "=" * 80)
print("PHASE 2: COUNTING RECORDS TO BE DELETED")
print("=" * 80)
print()

# Define all silver tables
silver_tables = [
    "fardap_silver_incidents",
    "fardap_silver_victim",
    "fardap_silver_vehicle",
    "fardap_silver_equipment",
    "fardap_silver_hazardousmaterial",
    "fardap_silver_buildingfacility",
    "fardap_silver_system",
    "fardap_silver_manualsystem",
    "fardap_silver_additionalinfo",
    "fardap_silver_qasummaries",
    "fardap_silver_validation",
    "fardap_silver_extrication",
    "fardap_silver_fireofficer"
]

# Create a temp view of duplicate documentIds for easier querying
duplicate_ids_df = spark.createDataFrame([(doc_id,) for doc_id in duplicate_doc_ids], ["documentId"])
duplicate_ids_df.createOrReplaceTempView("temp_duplicate_ids")

# Count records to be deleted from each table
deletion_counts = {}

for table in silver_tables:
    try:
        count = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM inc_fardap_lakehouse.{table}
            WHERE documentId IN (SELECT documentId FROM temp_duplicate_ids)
        """).first().cnt
        
        deletion_counts[table] = count
        print(f"{table:40s}: {count:6d} rows to delete")
    except Exception as e:
        print(f"{table:40s}: ERROR - {str(e)}")
        deletion_counts[table] = 0

total_to_delete = sum(deletion_counts.values())
print(f"\n{'TOTAL':40s}: {total_to_delete:6d} rows to delete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 3: DELETE DUPLICATES (UNCOMMENT TO EXECUTE)
# ============================================================================

print("\n" + "=" * 80)
print("PHASE 3: DELETING DUPLICATE RECORDS")
print("=" * 80)
print()

# IMPORTANT: Review the counts above before uncommenting this section!
# Uncomment the entire deletion section below to proceed:

"""
# Create WHERE clause with all duplicate documentIds
# For large lists, we'll delete in batches to avoid query size limits
BATCH_SIZE = 100
batches = [duplicate_doc_ids[i:i + BATCH_SIZE] for i in range(0, len(duplicate_doc_ids), BATCH_SIZE)]

print(f"Deleting {len(duplicate_doc_ids)} duplicate documentIds in {len(batches)} batches...")
print()

deletion_results = {}

# Delete from array tables FIRST (child records)
array_tables = [
    "fardap_silver_victim",
    "fardap_silver_vehicle",
    "fardap_silver_equipment",
    "fardap_silver_hazardousmaterial",
    "fardap_silver_buildingfacility",
    "fardap_silver_system",
    "fardap_silver_manualsystem",
    "fardap_silver_additionalinfo",
    "fardap_silver_qasummaries",
    "fardap_silver_validation",
    "fardap_silver_extrication",
    "fardap_silver_fireofficer"
]

print("--- Deleting from array tables ---")
for table in array_tables:
    try:
        total_deleted = 0
        for batch_idx, batch in enumerate(batches, 1):
            # Create WHERE clause for this batch
            doc_ids_str = ",".join([f"'{doc_id}'" for doc_id in batch])
            
            # Execute DELETE using Delta Lake
            spark.sql(f'''
                DELETE FROM inc_fardap_lakehouse.{table}
                WHERE documentId IN ({doc_ids_str})
            ''')
            
            # Count what was deleted (approximate - Delta doesn't return exact count)
            current_count = spark.sql(f"""
                SELECT COUNT(*) as cnt
                FROM inc_fardap_lakehouse.{table}
                WHERE documentId IN ({doc_ids_str})
            """).first().cnt
            
            if batch_idx == 1:
                print(f"  {table}: Processing batch {batch_idx}/{len(batches)}...", end="")
            
        # Final count check
        remaining = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM inc_fardap_lakehouse.{table}
            WHERE documentId IN (SELECT documentId FROM temp_duplicate_ids)
        """).first().cnt
        
        deleted = deletion_counts[table] - remaining
        deletion_results[table] = deleted
        print(f" ✓ Deleted {deleted} rows")
        
    except Exception as e:
        print(f"  {table}: ERROR - {str(e)}")
        deletion_results[table] = 0

print()
print("--- Deleting from main incidents table ---")

# Delete from incidents table LAST
try:
    total_deleted = 0
    for batch_idx, batch in enumerate(batches, 1):
        doc_ids_str = ",".join([f"'{doc_id}'" for doc_id in batch])
        
        spark.sql(f'''
            DELETE FROM inc_fardap_lakehouse.fardap_silver_incidents
            WHERE documentId IN ({doc_ids_str})
        ''')
        
        if batch_idx == 1:
            print(f"  fardap_silver_incidents: Processing batch {batch_idx}/{len(batches)}...", end="")
    
    # Final count check
    remaining = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM inc_fardap_lakehouse.fardap_silver_incidents
        WHERE documentId IN (SELECT documentId FROM temp_duplicate_ids)
    """).first().cnt
    
    deleted = deletion_counts["fardap_silver_incidents"] - remaining
    deletion_results["fardap_silver_incidents"] = deleted
    print(f" ✓ Deleted {deleted} rows")
    
except Exception as e:
    print(f"  fardap_silver_incidents: ERROR - {str(e)}")
    deletion_results["fardap_silver_incidents"] = 0

print()
print("=" * 80)
print("DELETION SUMMARY")
print("=" * 80)
print()
print("Rows deleted from each table:")
for table, count in deletion_results.items():
    print(f"  {table:40s}: {count:6d}")

total_deleted = sum(deletion_results.values())
print(f"\n{'TOTAL ROWS DELETED':40s}: {total_deleted:6d}")
print()
print("=" * 80)
print("✓ DELETION COMPLETE")
print("=" * 80)
print()
print("Next steps:")
print("1. Verify the deletions above look correct")
print("2. Run the Silver transformation notebook to recreate records from Bronze")
"""

print("\n⚠️  DELETION CODE IS COMMENTED OUT")
print("Review the counts above, then uncomment the deletion section to proceed.")

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
print("VERIFICATION: Checking for Remaining Duplicates")
print("=" * 80)
print()

remaining_duplicates = spark.sql("""
    SELECT 
        documentId,
        COUNT(*) as duplicate_count
    FROM inc_fardap_lakehouse.fardap_silver_incidents
    GROUP BY documentId
    HAVING COUNT(*) > 1
""")

remaining_count = remaining_duplicates.count()

if remaining_count == 0:
    print("✓ SUCCESS: No duplicates found in fardap_silver_incidents")
else:
    print(f"⚠️  WARNING: {remaining_count} documentIds still have duplicates")
    remaining_duplicates.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
