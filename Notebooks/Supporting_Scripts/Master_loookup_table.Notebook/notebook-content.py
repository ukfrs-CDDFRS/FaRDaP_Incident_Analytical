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

# CELL ********************

from pyspark.sql.functions import lit
from functools import reduce

# Get all tables in the lakehouse that start with 'lu'
tables = spark.sql("""
    SHOW TABLES IN inc_fardap_lakehouse
""").filter("tableName LIKE 'lu_%'")

# Create a list to hold DataFrames
dfs = []

# Loop through each table
for row in tables.collect():
    table_name = row.tableName
    
    # Read table and add source table name column
    df = spark.sql(f"""
        SELECT *,
               '{table_name}' AS source_table
        FROM inc_fardap_lakehouse.{table_name}
    """)
    
    dfs.append(df)

# Union all DataFrames together
combined_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)

# Display result
display(combined_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, regexp_replace

clean_df = combined_df

# Clean all string columns
for column_name, column_type in clean_df.dtypes:
    if column_type == "string":
        clean_df = clean_df.withColumn(
            column_name,
            regexp_replace(col(column_name), r'[\r\n]+', ' ')
        )

# Write single CSV
output_path = "Files/lu_combined_csv_clean"

clean_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("quoteAll", "true") \
    .option("escape", "\"") \
    .csv(output_path)

print(f"CSV saved to: {output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
