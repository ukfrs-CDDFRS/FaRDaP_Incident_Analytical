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

# # Update Silver Flatten State Watermark
# 
# **Purpose**: Reset the last_watermark to re-process records from a specific timestamp
# 
# **New Watermark**: 2026-06-18T14:31:33.356Z

# CELL ********************

# ============================================================================
# Show Current State
# ============================================================================

print("=" * 80)
print("CURRENT STATE")
print("=" * 80)
print()

current_state = spark.sql("""
    SELECT 
        total_flattened,
        last_watermark,
        mode
    FROM inc_fardap_lakehouse.fardap_silver_flatten_state
""")

current_state.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Update Watermark (UNCOMMENT TO EXECUTE)
# ============================================================================

print("\n" + "=" * 80)
print("UPDATING WATERMARK")
print("=" * 80)
print()

# UNCOMMENT THE CODE BELOW TO UPDATE THE WATERMARK:

"""
new_watermark = "2026-06-18T14:31:33.356Z"

spark.sql(f'''
    UPDATE inc_fardap_lakehouse.fardap_silver_flatten_state
    SET last_watermark = '{new_watermark}'
''')

print(f"✓ Watermark updated to: {new_watermark}")
"""

print("⚠️  UPDATE CODE IS COMMENTED OUT")
print("Uncomment the code above to execute the update.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Verify Update
# ============================================================================

print("\n" + "=" * 80)
print("UPDATED STATE")
print("=" * 80)
print()

updated_state = spark.sql("""
    SELECT 
        total_flattened,
        last_watermark,
        mode
    FROM inc_fardap_lakehouse.fardap_silver_flatten_state
""")

updated_state.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
