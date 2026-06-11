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

# # 🧹 00 Wipe Lakehouse and Full Load
# 
# **Purpose:** Complete lakehouse reset followed by full data load
# 
# This notebook will:
# 1. **Drop all tables** in Bronze, Silver, and Gold layers
# 2. **Clean up state tables** and CDC logs
# 3. **Trigger full load pipeline** (or run bronze load directly)
# 
# ⚠️ **WARNING:** This is a DESTRUCTIVE operation that will:
# - Delete ALL lakehouse tables
# - Remove ALL existing data
# - Reset ALL state tracking
# 
# **When to use:**
# - Initial setup / fresh start
# - Disaster recovery
# - After major schema changes
# - Data corruption recovery
# 
# **Duration:** 5-10 mins for wipe + 30 mins - several hours for full load

# MARKDOWN ********************

# ## Step 1: Configuration

# CELL ********************

from pyspark.sql import SparkSession
from datetime import datetime, timezone
import time

# Get the Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

# Configuration
LAKEHOUSE_NAME = vl.getVariable("LAKEHOUSE_NAME")

# Tables to drop - organized by layer
BRONZE_TABLES = [
    'fardap_bronze_incidents',
    'fardap_bronze_cdc_log',
    'fardap_sync_state'
]

SILVER_TABLES = [
    'fardap_silver_incidents',
    'fardap_silver_flatten_state',
    'fardap_silver_content_hash',
    'fardap_silver_cdc_log',
    # Array tables (will discover dynamically)
]

GOLD_TABLES = [
    'fardap_gold_incident_summary',
    'fardap_gold_daily_summary',
    'fardap_gold_by_category',
    'fardap_gold_cdc_log',
    'fardap_gold_state'
]

print(f'🔧 Wipe Configuration:')
print(f'   Lakehouse: {LAKEHOUSE_NAME}')
print(f'   Bronze tables: {len(BRONZE_TABLES)}')
print(f'   Silver tables: {len(SILVER_TABLES)} + dynamic arrays')
print(f'   Gold tables: {len(GOLD_TABLES)}')
print(f'\n⚠️  THIS WILL DELETE ALL DATA IN THE LAKEHOUSE')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Discover All Existing Tables
# 
# Before dropping, let's see what tables actually exist in the lakehouse.

# CELL ********************

# Get all existing tables in the lakehouse
all_tables = spark.catalog.listTables()
existing_table_names = [t.name for t in all_tables]

print(f'📊 Found {len(existing_table_names)} existing tables in lakehouse:\n')

# Categorize tables
bronze_found = [t for t in existing_table_names if 'bronze' in t.lower()]
silver_found = [t for t in existing_table_names if 'silver' in t.lower()]
gold_found = [t for t in existing_table_names if 'gold' in t.lower()]
other_found = [t for t in existing_table_names if t not in bronze_found + silver_found + gold_found]

print(f'🥉 Bronze tables ({len(bronze_found)}):')
for t in bronze_found:
    print(f'   - {t}')

print(f'\n🥈 Silver tables ({len(silver_found)}):')
for t in silver_found:
    print(f'   - {t}')

print(f'\n🥇 Gold tables ({len(gold_found)}):')
for t in gold_found:
    print(f'   - {t}')

if other_found:
    print(f'\n📦 Other tables ({len(other_found)}):')
    for t in other_found:
        print(f'   - {t}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Drop All Tables
# 
# Drop tables in reverse order: Gold → Silver → Bronze

# CELL ********************

def drop_table_safe(table_name):
    """Safely drop a table if it exists"""
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"   ✅ Dropped: {table_name}")
        return True
    except Exception as e:
        print(f"   ⚠️  Failed to drop {table_name}: {str(e)}")
        return False

print(f'\n🧹 Starting table wipe at {datetime.now(timezone.utc).isoformat()}\n')

# Drop Gold tables first
print('🥇 Dropping GOLD tables...')
gold_dropped = 0
for table in gold_found:
    if drop_table_safe(table):
        gold_dropped += 1
        time.sleep(0.5)  # Brief pause between drops

# Drop Silver tables (including dynamic array tables)
print(f'\n🥈 Dropping SILVER tables...')
silver_dropped = 0
for table in silver_found:
    if drop_table_safe(table):
        silver_dropped += 1
        time.sleep(0.5)

# Drop Bronze tables last
print(f'\n🥉 Dropping BRONZE tables...')
bronze_dropped = 0
for table in bronze_found:
    if drop_table_safe(table):
        bronze_dropped += 1
        time.sleep(0.5)

# Drop any other tables
if other_found:
    print(f'\n📦 Dropping OTHER tables...')
    other_dropped = 0
    for table in other_found:
        if drop_table_safe(table):
            other_dropped += 1
            time.sleep(0.5)

print(f'\n✅ Wipe complete!')
print(f'   Gold tables dropped: {gold_dropped}/{len(gold_found)}')
print(f'   Silver tables dropped: {silver_dropped}/{len(silver_found)}')
print(f'   Bronze tables dropped: {bronze_dropped}/{len(bronze_found)}')
if other_found:
    print(f'   Other tables dropped: {other_dropped}/{len(other_found)}')
print(f'\n🎉 Lakehouse is now empty and ready for full load')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Verify Clean State

# CELL ********************

# Verify all tables are gone
remaining_tables = spark.catalog.listTables()
remaining_count = len(remaining_tables)

if remaining_count == 0:
    print('✅ SUCCESS: All tables have been dropped')
    print('   Lakehouse is completely empty')
else:
    print(f'⚠️  WARNING: {remaining_count} tables still remain:')
    for t in remaining_tables:
        print(f'   - {t.name}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Trigger Full Load
# 
# Now that the lakehouse is clean, initiate the full load process.
# 
# **Option A:** Run the pipeline (recommended for orchestration)
# **Option B:** Run Bronze Full Load notebook directly (faster for testing)

# CELL ********************

print('📋 Full Load Options:\n')
print('Option A - Run Full Load Pipeline (Recommended):')
print('   Navigate to: PL_FaRDaP_inc_full_load')
print('   Click "Run" to execute full Bronze → Silver → Gold workflow')
print('')
print('Option B - Run Bronze Full Load Notebook Directly:')
print('   Navigate to: 01_Bronze_Full_Load')
print('   Run notebook to load Bronze data')
print('   Then manually run 02_Silver_Full_Transform_Enhanced')
print('   Then manually run 03_Gold_Full_Analytics')
print('')
print('💡 TIP: Option A (pipeline) is recommended for production workflows')
print('')

# Uncomment to automatically trigger Bronze full load from here:
# %run 01_Bronze_Full_Load

print('✅ Wipe complete. Ready to run full load pipeline.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Next Steps
# 
# 1. ✅ **Wipe Complete** - All tables dropped
# 2. 🔄 **Run Full Load** - Execute pipeline or Bronze notebook
# 3. 🔍 **Validate** - Check record counts after load
# 4. 🚀 **Resume Normal Operations** - Switch to incremental loads
# 
# ---
# 
# **Estimated Timeline:**
# - Bronze Full Load: 30 mins - 2 hours
# - Silver Transform: 10-30 mins
# - Gold Analytics: 5-10 mins
# 
# Total: ~45 mins - 3 hours depending on data volume
