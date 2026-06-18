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

# # 🔄 00 Migrate State Table to Dual-Watermark Schema
# 
# **Purpose:** One-time migration from single watermark to dual-watermark tracking
# 
# ## What This Does:
# 
# Migrates `fardap_sync_state` from:
# ```
# last_watermark (single timestamp)
# ```
# 
# To:
# ```
# last_watermark_territory   (territoryFrsId watermark)
# last_watermark_responsible (responsibleFrsId watermark)
# ```
# 
# Both new columns are initialized with the current `last_watermark` value to ensure no data loss.
# 
# ## When to Run:
# - **ONCE ONLY** before deploying dual-search notebooks
# - After backing up current state (optional but recommended)
# - Before running updated Bronze Full Load or Incremental Sync
# 
# ## Safety:
# - ✅ Non-destructive: Preserves existing watermark value
# - ✅ Idempotent: Safe to re-run (checks if already migrated)
# - ✅ Validates migration success before completion
# 
# ## Duration:
# < 1 minute

# MARKDOWN ********************

# ## Step 1: Configuration and Setup

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timezone

# Get Spark session
spark = SparkSession.builder.getOrCreate()

# Get Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

# Configuration
LAKEHOUSE_NAME = vl.getVariable("LAKEHOUSE_NAME")
STATE_TABLE = "fardap_sync_state"

print(f"🔧 Configuration:")
print(f"   Lakehouse: {LAKEHOUSE_NAME}")
print(f"   State Table: {STATE_TABLE}")
print(f"   Migration Date: {datetime.now(timezone.utc).isoformat()}")

# MARKDOWN ********************

# ## Step 2: Check Current Schema

# CELL ********************

# Read current state table
try:
    current_state_df = spark.table(STATE_TABLE)
    print("✅ Successfully read fardap_sync_state table")
    print(f"\n📊 Current row count: {current_state_df.count()}")
    
    # Show current schema
    print(f"\n📋 Current schema:")
    current_state_df.printSchema()
    
    # Show current data
    print(f"\n📄 Current state:")
    current_state_df.show(truncate=False)
    
    # Check if already migrated
    current_columns = current_state_df.columns
    if "last_watermark_territory" in current_columns and "last_watermark_responsible" in current_columns:
        print("\n⚠️  WARNING: Dual-watermark columns already exist!")
        print("   This table appears to have been migrated already.")
        print("   Review the data above to confirm.")
        ALREADY_MIGRATED = True
    else:
        print("\n✅ Table has not been migrated yet - ready to proceed")
        ALREADY_MIGRATED = False
        
except Exception as e:
    print(f"❌ ERROR: Could not read state table: {e}")
    raise

# MARKDOWN ********************

# ## Step 3: Perform Migration
# 
# **This cell will:**
# 1. Add `last_watermark_territory` column
# 2. Add `last_watermark_responsible` column
# 3. Initialize both with current `last_watermark` value
# 4. Keep original `last_watermark` column for backwards compatibility (deprecated)
# 5. Overwrite the table with new schema

# CELL ********************

if ALREADY_MIGRATED:
    print("⏭️  SKIPPING MIGRATION: Table already has dual-watermark schema")
else:
    print("🔄 Starting migration...")
    
    try:
        # Create new DataFrame with dual watermarks
        # Initialize both new columns with the existing watermark value
        migrated_df = current_state_df.withColumn(
            "last_watermark_territory", 
            col("last_watermark")
        ).withColumn(
            "last_watermark_responsible", 
            col("last_watermark")
        )
        
        # Reorder columns: keep last_watermark first for compatibility
        # then add the two new watermark columns
        column_order = [
            "last_watermark",              # Original (deprecated but kept)
            "last_watermark_territory",    # New: territoryFrsId watermark
            "last_watermark_responsible"   # New: responsibleFrsId watermark
        ]
        
        migrated_df = migrated_df.select(*column_order)
        
        print("\n📋 New schema (preview):")
        migrated_df.printSchema()
        
        print("\n📄 New data (preview):")
        migrated_df.show(truncate=False)
        
        # Save the migrated table (overwrite mode with schema evolution)
        print(f"\n💾 Writing migrated table to {STATE_TABLE}...")
        migrated_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(STATE_TABLE)
        
        print("✅ Migration completed successfully!")
        
    except Exception as e:
        print(f"❌ ERROR during migration: {e}")
        raise

# MARKDOWN ********************

# ## Step 4: Validate Migration

# CELL ********************

print("🔍 Validating migration...")

try:
    # Re-read the table
    validated_df = spark.table(STATE_TABLE)
    
    # Check schema
    print("\n📋 Final schema:")
    validated_df.printSchema()
    
    # Check data
    print("\n📄 Final state:")
    validated_df.show(truncate=False)
    
    # Validation checks
    columns = validated_df.columns
    row = validated_df.collect()[0]
    
    checks_passed = []
    checks_failed = []
    
    # Check 1: Both new columns exist
    if "last_watermark_territory" in columns:
        checks_passed.append("✅ last_watermark_territory column exists")
    else:
        checks_failed.append("❌ last_watermark_territory column missing")
    
    if "last_watermark_responsible" in columns:
        checks_passed.append("✅ last_watermark_responsible column exists")
    else:
        checks_failed.append("❌ last_watermark_responsible column missing")
    
    # Check 2: Values are not null
    if row["last_watermark_territory"] is not None:
        checks_passed.append("✅ last_watermark_territory has value")
    else:
        checks_failed.append("❌ last_watermark_territory is NULL")
    
    if row["last_watermark_responsible"] is not None:
        checks_passed.append("✅ last_watermark_responsible has value")
    else:
        checks_failed.append("❌ last_watermark_responsible is NULL")
    
    # Check 3: Both watermarks match original (if not already migrated)
    original_watermark = row["last_watermark"]
    territory_watermark = row["last_watermark_territory"]
    responsible_watermark = row["last_watermark_responsible"]
    
    if original_watermark == territory_watermark:
        checks_passed.append("✅ Territory watermark matches original")
    else:
        checks_failed.append(f"⚠️  Territory watermark differs: {territory_watermark} vs {original_watermark}")
    
    if original_watermark == responsible_watermark:
        checks_passed.append("✅ Responsible watermark matches original")
    else:
        checks_failed.append(f"⚠️  Responsible watermark differs: {responsible_watermark} vs {original_watermark}")
    
    # Print results
    print("\n" + "="*60)
    print("📊 VALIDATION RESULTS")
    print("="*60)
    
    for check in checks_passed:
        print(check)
    
    for check in checks_failed:
        print(check)
    
    print("="*60)
    
    if len(checks_failed) == 0:
        print("\n🎉 SUCCESS! Migration completed and validated.")
        print("\n📋 Next steps:")
        print("   1. Update Bronze Full Load notebook (01_Bronze_Full_Load)")
        print("   2. Update Bronze Incremental Sync notebook (01_Bronze_Incremental_Sync)")
        print("   3. Run backfill notebook to fetch historical responsible incidents")
    else:
        print("\n⚠️  WARNING: Some validation checks failed.")
        print("   Review the results above before proceeding.")
        
except Exception as e:
    print(f"❌ ERROR during validation: {e}")
    raise

# MARKDOWN ********************

# ## Step 5: Summary
# 
# ### What Changed:
# 
# **Before Migration:**
# ```python
# fardap_sync_state
# ├── last_watermark: timestamp  # Single watermark
# ```
# 
# **After Migration:**
# ```python
# fardap_sync_state
# ├── last_watermark: timestamp              # Kept for compatibility (deprecated)
# ├── last_watermark_territory: timestamp    # For territoryFrsId search
# └── last_watermark_responsible: timestamp  # For responsibleFrsId search
# ```
# 
# ### Rollback (if needed):
# 
# If you need to rollback this migration:
# 
# ```python
# # Option 1: Keep only original watermark column
# df = spark.table("fardap_sync_state")
# df.select("last_watermark").write.format("delta").mode("overwrite").saveAsTable("fardap_sync_state")
# 
# # Option 2: Restore from backup (if you created one)
# # spark.table("fardap_sync_state_backup").write.format("delta").mode("overwrite").saveAsTable("fardap_sync_state")
# ```
# 
# ### Migration Complete! ✅
# 
# You can now proceed to Phase 2: Updating the Bronze notebooks to use dual-search logic.

# MARKDOWN ********************

# ---
# 
# **Notebook:** 00_Migrate_State_Table  
# **Purpose:** One-time migration to dual-watermark schema  
# **Version:** 1.0  
# **Created:** 2026-06-18  
# **Phase:** Dual-Search Bronze Ingestion (Phase 1)
