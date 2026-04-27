# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## SQL Query Transformer
# 
# Converts a compact SQL string — using `↵` as line separators and `{lakehouse_name}` as a schema placeholder — into formatted T-SQL with:
# - Proper newlines
# - A resolved `[schema].[dbo]` prefix in place of `{lakehouse_name}`
# - Square-bracket-wrapped column aliases (e.g. `AS [Date]`)

# CELL ********************

# ── Configuration ─────────────────────────────────────────────────────────────
# Set the target schema prefix that replaces {lakehouse_name} in the query.
TARGET_SCHEMA = "[LH_PS_IRS].[dbo]"

# Paste your raw query string here (↵ as newline, {lakehouse_name} as placeholder)
RAW_SQL = (
    "SELECT↵"
    "    'PI02' AS MeasureCode,↵"
    "    inc_incident.inc_pk AS Key,↵"
    "    inc_date_created AS Date,↵"
    "    COUNT(inc_incident.inc_pk) AS Count,↵"
    "    inc_geo.STN_code AS Section_Name↵"
    "FROM ↵"
    "    {lakehouse_name}.fire AS fire↵"
    "JOIN ↵"
    "    {lakehouse_name}.inc_incident AS inc_incident ON fire.fire_fk_inc = inc_incident.inc_pk↵"
    "JOIN↵"
    "    {lakehouse_name}.inc_geographies as inc_geo ON inc_incident.inc_pk = inc_geo.inc_pk↵"
    "WHERE ↵"
    "    inc_incident.inc_fire_classification = '1' AND  -- primary fire↵"
    "    inc_incident.inc_incident_category = 1  -- fire↵"
    "GROUP BY↵"
    "      inc_incident.inc_pk,↵"
    "      inc_date_created,↵"
    "      inc_geo.STN_code;"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re

def transform_sql(raw_sql: str, target_schema: str = "[LH_PS_IRS].[dbo]") -> str:
    """
    Transforms a compact SQL string into formatted T-SQL.

    Steps:
      1. Replace ↵ with real newlines.
      2. Replace {lakehouse_name}. with the target schema prefix.
      3. Bracket column aliases (AS word) that are:
         - followed by a comma  (mid-SELECT-list alias), or
         - at the end of the SELECT list (line before FROM / WHERE /
           GROUP / HAVING / ORDER / UNION).
         Table aliases (AS x ON … or AS x ↵ JOIN …) are left unchanged.
    """
    # Step 1 – restore newlines
    sql = raw_sql.replace("↵", "\n")

    # Step 2 – resolve lakehouse placeholder
    sql = sql.replace("{lakehouse_name}.", target_schema + ".")

    # Step 3a – bracket aliases followed by a comma  →  AS [alias],
    sql = re.sub(
        r"\bAS\s+(\w+)(?=\s*,)",
        r"AS [\1]",
        sql,
        flags=re.IGNORECASE,
    )

    # Step 3b – bracket the last alias in a SELECT list
    #   Match: AS <word>  followed by optional whitespace / newlines then a
    #   top-level clause keyword.  This handles the final column alias that
    #   has no trailing comma.
    sql = re.sub(
        r"\bAS\s+(\w+)(?=\s*\n\s*(?:FROM|WHERE|GROUP|HAVING|ORDER|UNION)\b)",
        r"AS [\1]",
        sql,
        flags=re.IGNORECASE,
    )

    return sql

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = transform_sql(RAW_SQL, TARGET_SCHEMA)
print(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
