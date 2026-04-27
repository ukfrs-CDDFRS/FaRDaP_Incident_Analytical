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

df = spark.sql("SELECT documentid, raw_json FROM inc_fardap_lakehouse.fardap_raw_incidents_full")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

json_df = spark.read.json(
    spark.sql("""
        SELECT raw_json
        FROM inc_fardap_lakehouse.fardap_raw_incidents_full
    """).rdd.map(lambda r: r.raw_json)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

json_df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = json_df.schema
schema


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def flatten_schema(schema, prefix=""):
    fields = []
    for f in schema.fields:
        name = f"{prefix}.{f.name}" if prefix else f.name
        if hasattr(f.dataType, "fields"):
            fields.extend(flatten_schema(f.dataType, name))
        else:
            fields.append((name, f.dataType.simpleString()))
    return fields

field_inventory = flatten_schema(json_df.schema)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.createDataFrame(field_inventory, ["json_path", "data_type"])
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
