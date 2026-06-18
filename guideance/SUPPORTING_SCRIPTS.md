# Supporting Scripts

> Utility notebooks for exploration, troubleshooting, and deep analysis

---

## Table of Contents

- [Overview](#overview)
- [Create Lookup Tables](#create-lookup-tables)
- [Find Your FRS ID](#find-your-frs-id)
- [Explore Package Schema](#explore-package-schema)
- [RawJson Explorer](#rawjson-explorer)
- [Incident Deep Dive](#incident-deep-dive)

---

## Overview

These supporting notebooks help with initial setup, data exploration, and troubleshooting. They are **not part of the automated pipeline** but are essential for understanding and configuring the platform.

| Notebook | Purpose | When to Use |
|:---------|:--------|:------------|
| `00_Migrate_State_Table` | Migrate state table to dual-watermark schema | One-time migration to dual-search |
| `01_Backfill_Responsible_Incidents` | Capture historical cross-border incidents | One-time after dual-search upgrade |
| `01_Create_Lookup_Tables` | Create Delta lookup tables from controlled lists | Initial setup, periodic refresh |
| `Find_Your_FRS_ID` | Lookup your organisation's FRS ID | Initial setup |
| `Explore_Package_Schema` | View FaRDaP API data structure | Understanding data model |
| `Explore_Controlled_Lists` | Browse reference data / valid values | Data validation, reporting |
| `FaRDaP_Schema_Reference_Data` | Build field mappings from schema | Reports with display names |
| `RawJson_explorer` | Examine raw Bronze data | Debugging, analysis |
| `Incident_Deep_Dive` | Detailed incident analysis | Troubleshooting, research |
| `dup_frsincidentnumber` | Find duplicate incident numbers | Data quality checks |

---

## Backfill Responsible Incidents

📁 **Location:** `Supporting_Scripts/01_Backfill_Responsible_Incidents.Notebook`

### Purpose

One-time backfill to capture historical cross-border incidents missed before dual-search implementation

### When to Run

Once after upgrading to dual-search (after Full Load and Incremental notebooks updated)

### What It Does

1. Searches ALL `responsibleFrsId=17` incidents (no date filter)
2. Anti-joins against existing Bronze table (`Bronze IDs - Responsible IDs`)
3. Fetches only the missing incidents (~250-700 cross-border responses)
4. Appends to Bronze table (mode='append')
5. Logs to CDC with `op_type='backfill_insert'` for audit trail

### Expected Results

```
Existing Bronze incidents:     132,000
Responsible incidents (total): 132,500
─────────────────────────────────────
Already have:                  131,800  (overlap)
Missing (to backfill):           700    (cross-border responses)
```

### Safety Features

- **Anti-join:** Only inserts incidents NOT already in Bronze
- **Append mode:** Never overwrites existing data
- **Distinct audit trail:** CDC logs as 'backfill_insert' for tracking
- **Idempotent:** Safe to re-run (anti-join will find 0 missing incidents)

### Output

- Bronze table: +250-700 incidents
- CDC log: +250-700 rows with `op_type='backfill_insert'`
- Console: Incident type breakdown showing cross-border captures

### Verification Query

```sql
-- Check backfill results
SELECT 
  documentId,
  get_json_object(raw_json, '$.content.incident.territoryFrsId') as territory_frs,
  get_json_object(raw_json, '$.content.incident.responsibleFrsId') as responsible_frs,
  sync_timestamp
FROM fardap_bronze_incidents
WHERE get_json_object(raw_json, '$.content.incident.territoryFrsId') != '17'
  AND get_json_object(raw_json, '$.content.incident.responsibleFrsId') = '17'
LIMIT 20;
-- Expected: ~250-700 rows showing cross-border incidents
```

**Note:** This is a **ONE-TIME** operation. After backfill completes, the regular incremental pipeline (with dual-search) will automatically capture all future cross-border incidents.

## Create Lookup Tables

📁 **Location:** `Supporting_Scripts/01_Create_Lookup_Tables.Notebook`

### Purpose

Fetches **all controlled lists** from the FaRDaP™ API and creates **Delta lookup tables** in the Lakehouse for each one. These tables enable you to join incident data with human-readable reference values.

### When to Run

| Scenario | Run? |
|:---------|:-----|
| Initial system setup | ✅ Yes |
| After FaRDaP releases new controlled list versions | ✅ Yes |
| Periodic refresh (quarterly recommended) | ✅ Yes |
| Regular pipeline execution | ❌ No |

### What It Does

1. **Authenticates** with the FaRDaP™ API using Key Vault credentials
2. **Fetches all controlled lists** (70+) with complete item data
3. **Creates lookup tables** named `LU_{ListTypeName}` for each list
4. **Creates master reference** table `LU_Master_Reference` documenting all lookups

### Tables Created

| Table Name | Description |
|:-----------|:------------|
| `LU_FRSIdListType` | Fire & Rescue Service organisations |
| `LU_IncidentCategoryType` | Types of incidents |
| `LU_PropertyCategoryType` | Building classifications |
| `LU_IncidentCauseType` | Reasons fires started |
| `LU_StationIdListType` | Fire station identifiers |
| `LU_ApplianceIdListType` | Appliance/vehicle identifiers |
| `LU_Master_Reference` | Index of all lookup tables |
| ... | 70+ additional controlled lists |

### Lookup Table Schema

Each `LU_{ListName}` table contains:

| Column | Type | Description |
|:-------|:-----|:------------|
| `id` | String | Unique identifier for the item |
| `name` | String | Display name |
| `external_id` | String | External reference ID |
| `obsolete` | Boolean | Whether the item is deprecated |
| `guidance` | String | Usage guidance text |
| `keywords` | String | Search keywords |
| `broader_item_id` | String | Parent item ID (hierarchical lists) |
| `broader_item_name` | String | Parent item name |
| `attributes_json` | String | JSON array of attributes |
| `added_in_version` | String | Version when item was added |
| `last_updated_in_version` | String | Version when last updated |
| `list_version` | String | Version of the controlled list |
| `loaded_at` | Timestamp | When data was loaded |
| `extra_*` | String | Dynamic fields from API |

### Dynamic Schema Handling

The notebook handles **schema evolution automatically**:
- Known fields map to standard column names
- New/unexpected API fields are captured with `extra_` prefix
- Complex objects/arrays are serialized as JSON strings
- Schema evolution enabled - new columns won't break existing tables

### Usage Example

```sql
-- Join incident data with lookup tables for readable reports
SELECT 
    i.content_identifier_frsincidentnumber AS incident_number,
    frs.name AS fire_service_name,
    cat.name AS incident_category,
    cause.name AS incident_cause
FROM incidents i
LEFT JOIN LU_FRSIdListType frs ON i.frs_id = frs.id
LEFT JOIN LU_IncidentCategoryType cat ON i.incident_category_id = cat.id
LEFT JOIN LU_IncidentCauseType cause ON i.incident_cause_id = cause.id
```

### Process Flow

```
Step 1: Configuration & Authentication
├── Load variables from Fabric Variable Library
├── Retrieve secrets from Azure Key Vault
└── Authenticate with FaRDaP API

Step 2: Fetch All Controlled Lists
├── GET /api/v1/metadata/reference-data/controlled-lists
├── Iterate through all lists (paginated)
└── Fetch complete item data for each list

Step 3: Create Lookup Tables
├── Flatten each item (handle dynamic schema)
├── Create Spark DataFrame
└── Write as Delta table with overwriteSchema=true

Step 4: Verify Created Tables
├── List all LU_* tables
└── Display row counts

Step 5: Create Master Reference
└── Save LU_Master_Reference with metadata
```

---

## Find Your FRS ID

📁 **Location:** `Supporting_Scripts/00_Find_Your_FRS_ID.Notebook`

### Purpose

Helps you identify the correct **numeric FRS ID** for your Fire and Rescue Service organisation. This is critical for configuring the Variable Library.

### Why This Matters

The FaRDaP API requires the **numeric FRS ID**, not the two-character IRS code:

| ✅ Correct (FRS ID) | ❌ Wrong (IRS Code) |
|:-------------------|:-------------------|
| `17` | `HS` |
| `28` | `GM` |
| `42` | `WY` |

### What It Does

1. **Authenticates** with FaRDaP API using Key Vault credentials
2. **Fetches** the `FRSIdListType` Reference Data
3. **Displays** all Fire and Rescue Services with their IDs
4. **Tests** your configured FRS ID

### Sample Output

```
Available Fire & Rescue Services:
┌────┬────────────────────────────┬──────────┐
│ ID │ Name                       │ IRS Code │
├────┼────────────────────────────┼──────────┤
│ 17 │ Humberside                 │ HS       │
│ 28 │ Greater Manchester         │ GM       │
│ 39 │ London South               │ LS       │
│ 42 │ West Yorkshire             │ WY       │
└────┴────────────────────────────┴──────────┘
```

### Usage

```python
# Run all cells in sequence
# 1. Authenticate and get token
# 2. Fetch FRS list
# 3. Display results
# 4. Update your Variable Library with the correct FRS ID
```

---

## Explore Package Schema

📁 **Location:** `Supporting_Scripts/00_Explore_Package_Schema.Notebook`

### Purpose

Explores the FaRDaP API schema to understand available data structures, field types, and relationships.

### What It Does

1. **Fetches** package/schema metadata from API
2. **Parses** field definitions and data types
3. **Identifies** array fields for Silver layer flattening
4. **Documents** available reference data types

### Key Information Revealed

- **Top-level fields** that become columns in `incidents` table
- **Array fields** that become separate Silver tables
- **Reference data types** for lookups and validation
- **Field data types** (string, number, date, etc.)

### When to Use

- Understanding what data is available
- Planning custom analytics
- Debugging transformation issues
- Identifying new fields after API updates

### Sample Schema Output

> ℹ️ **Note:** This shows the **API schema** (raw data structure). The `documentId` field is used consistently across Bronze and Silver layers.

```
Schema Fields (from API):
├── documentId (integer) - Document identifier
├── incidentNumber (string) 
├── incidentDateTime (dateTime)
├── incidentType (string) 
├── victim (array) ← Silver table
│   ├── victimType (string)
│   ├── gender (string)
│   └── age (integer)
├── vehicle (array) ← Silver table
│   ├── vehicleType (string)
│   └── registrationYear (integer)
└── ...
```

---

## Explore Controlled Lists

📁 **Location:** `Supporting_Scripts/Explore_Controlled_Lists.Notebook`

### Purpose

Browses all **Controlled Lists** (reference data) from the FaRDaP API. These are lookup tables that define valid values for fields in incident documents.

### What are Controlled Lists?

Controlled Lists act like enumerations or lookup tables — ensuring data consistency across the system. Examples include:

| List Type | Purpose |
|:----------|:--------|
| `FRSIdListType` | All Fire & Rescue Service organisations |
| `IncidentCategoryType` | Types of incidents (fire, rescue, false alarm) |
| `PropertyCategoryType` | Building classifications |
| `IncidentCauseType` | Reasons fires started |
| `VictimSeverityType` | Injury severity levels |

### What It Does

1. **Authenticates** with FaRDaP API
2. **Fetches** all available controlled lists
3. **Retrieves** every item within each list
4. **Analyzes** schema structure against OpenAPI specification
5. **Displays** summary tables with item counts

### When to Use

- Understanding what values are valid for specific fields
- Building reports that need user-friendly labels
- Validating data quality
- Creating dropdown/filter options in applications
- Mapping codes to descriptions

### Sample Output

```
Found 47 controlled lists.

Fetching detailed structure for each list...

  [1/47] FRSIdListType: 51 items ✓
  [2/47] IncidentCategoryType: 12 items ✓
  [3/47] PropertyCategoryType: 87 items ✓
  [4/47] IncidentCauseType: 34 items ✓
  ...

✓ Successfully loaded 47/47 controlled lists!

════════════════════════════════════════════════════════════
ALL LISTS SUMMARY TABLE
════════════════════════════════════════════════════════════

┌─────────────────────────┬─────────┬────────────┬─────────────┬─────────┐
│ List Type Name          │ Version │ Item Count │ Field Count │ Status  │
├─────────────────────────┼─────────┼────────────┼─────────────┼─────────┤
│ FRSIdListType           │ 1.0     │ 51         │ 9           │ SUCCESS │
│ IncidentCategoryType    │ 1.0     │ 12         │ 9           │ SUCCESS │
│ PropertyCategoryType    │ 2.1     │ 87         │ 9           │ SUCCESS │
│ VictimSeverityType      │ 1.0     │ 5          │ 9           │ SUCCESS │
└─────────────────────────┴─────────┴────────────┴─────────────┴─────────┘
```

### Item Structure

Each controlled list item contains:

| Field | Description |
|:------|:------------|
| `id` | Unique identifier |
| `name` | Display name |
| `externalId` | External reference code |
| `addedInVersion` | When item was added |
| `lastUpdatedInVersion` | Last modification version |
| `broaderItem` | Parent item (for hierarchical lists) |
| `obsolete` | Whether item is deprecated |
| `guidance` | Usage guidance notes |
| `keywords` | Search keywords |
| `attributes` | Additional metadata |

### Usage Example

```python
# After running the notebook, access a specific list:
frs_list = all_lists_raw['FRSIdListType']

# Get all items
for item in frs_list['items']:
    print(f"{item['id']}: {item['name']}")

# Output:
# 17: Humberside Fire and Rescue Service
# 28: Greater Manchester Fire and Rescue Service
# 42: West Yorkshire Fire and Rescue Service
```

---

## FaRDaP Schema Reference Data

📁 **Location:** `Supporting_Scripts/FaRDaP_Schema_Reference_Data.Notebook`

### Purpose

Builds **field mappings** from schema definitions and controlled lists, enabling you to transform coded field values into human-readable display names.

### What It Does

1. **Retrieves** available reference data packages from API
2. **Fetches** the latest Incident schema package
3. **Lists** all controlled lists with item counts
4. **Creates** mapping dictionaries (ID → display name)
5. **Provides** enrichment functions for incident data

### Key Difference from Explore_Controlled_Lists

| Notebook | Focus |
|:---------|:------|
| `Explore_Controlled_Lists` | Browsing and understanding list contents |
| `FaRDaP_Schema_Reference_Data` | Building usable mappings for data transformation |

### When to Use

- Building reports that need display names instead of codes
- Creating lookup tables for Power BI
- Transforming incident data for end-user consumption
- Enriching data with guidance text and attributes

### Sample Output

```
Incident Schema Package
  Name: Incident
  Version: 3.2
  Build Time: 2025-11-15T10:30:00Z

Schema Entries (47):
  - IncidentType (v1.0)
  - PropertyCategory (v2.1)
  - VictimSeverity (v1.0)
  ...

Controlled List: IncidentType
Version: 1.0

Items (12):
  ID: 1
    Name (Display): Primary Fire
    External ID: PF
    Guidance: Fire requiring attendance...

  ID: 2
    Name (Display): Secondary Fire
    External ID: SF
    ...
```

### Enrichment Function

The notebook provides a reusable function:

```python
def enrich_incident_with_display_names(incident_content, mappings):
    """
    Transform incident data by replacing coded values with display names
    """
    enriched = incident_content.copy()
    
    # Map IncidentType field
    if 'IncidentType' in incident_content and 'IncidentType' in mappings:
        incident_type_id = incident_content.get('IncidentType')
        if incident_type_id in mappings['IncidentType']:
            enriched['IncidentType_Display'] = mappings['IncidentType'][incident_type_id]['display_name']
    
    return enriched

# Usage:
# enriched_incident = enrich_incident_with_display_names(incident, field_mappings)
```

---

## RawJson Explorer

📁 **Location:** `Supporting_Scripts/00_RawJson_explorer.Notebook`

### Purpose

Examines the raw JSON data stored in the Bronze layer, allowing you to inspect actual API responses.

### What It Does

1. **Reads** from `fardap_bronze_incidents` table
2. **Parses** JSON from `raw_json` column
3. **Pretty prints** structured data
4. **Enables** ad-hoc exploration

### When to Use

- Debugging Silver layer transformations
- Understanding actual data content (not just schema)
- Finding edge cases in data
- Verifying data quality

### Sample Usage

```python
# Read a specific incident from Bronze layer (uses documentId)
df = spark.sql("""
    SELECT raw_json 
    FROM fardap_bronze_incidents 
    WHERE documentId = 123456
""")

# Parse and display
import json
raw = df.first()['raw_json']
data = json.loads(raw)
print(json.dumps(data, indent=2))
```

### Example Output

```json
{
  "documentId": 123456,
  "incidentNumber": "FRS/2024/00567",
  "incidentType": "Primary Fire",
  "incidentDateTime": "2024-01-15T14:30:00Z",
  "victim": [
    {
      "victimType": "Casualty",
      "gender": "Male",
      "age": 45,
      "severity": "Minor"
    }
  ],
  "vehicle": [],
  "equipment": [
    {
      "equipmentType": "Hose Reel",
      "quantity": 2
    }
  ]
}
```

---

## Incident Deep Dive

📁 **Location:** `Supporting_Scripts/Incident_Deep_Dive.Notebook`

### Purpose

Provides detailed analysis of specific incidents by joining across all Silver tables.

### What It Does

1. **Accepts** an incident identifier (documentId or incident number)
2. **Queries** all 11 Silver tables
3. **Joins** data on `documentId`
4. **Displays** comprehensive incident view

### When to Use

- Investigating specific incidents
- Validating data relationships
- Checking data completeness
- Supporting operational queries

### Sample Usage

```python
# Set the incident to investigate (use documentId from Silver layer)
DOCUMENT_ID = 123456

# Or query by incident number
INCIDENT_NUMBER = "FRS/2024/00567"

# The notebook will display:
# - Incident details (from incidents table)
# - All victims (from victim table)
# - All vehicles (from vehicle table)
# - Equipment used (from equipment table)
# - Systems present (from system table)
# - And more...
```

### Sample Output

```
═══════════════════════════════════════════════════════════
                   INCIDENT DEEP DIVE
═══════════════════════════════════════════════════════════
Document ID: 123456
Number:      FRS/2024/00567
Type:     Primary Fire
Date:     2024-01-15 14:30:00
Duration: 127 minutes
───────────────────────────────────────────────────────────
VICTIMS (1)
───────────────────────────────────────────────────────────
│ Type     │ Gender │ Age │ Severity │
│ Casualty │ Male   │ 45  │ Minor    │
───────────────────────────────────────────────────────────
EQUIPMENT USED (3)
───────────────────────────────────────────────────────────
│ Equipment    │ Quantity │
│ Hose Reel    │ 2        │
│ BA Set       │ 4        │
│ Ladder       │ 1        │
───────────────────────────────────────────────────────────
SYSTEMS PRESENT (2)
───────────────────────────────────────────────────────────
│ System        │ Status      │ Activated │
│ Smoke Alarm   │ Operational │ Yes       │
│ Sprinkler     │ Operational │ No        │
═══════════════════════════════════════════════════════════
```

---

## Duplicate Incident Number Finder

📁 **Location:** `Supporting_Scripts/dup_frsincidentnumber.Notebook`

### Purpose

Identifies **duplicate FRS incident numbers** in the Silver layer for data quality validation.

### What It Does

1. **Reads** from `fardap_silver_incidents` table
2. **Groups** by `content_identifier_frsincidentnumber`
3. **Filters** for groups with count > 1 (duplicates)
4. **Displays** all duplicate records for investigation

### When to Use

- Data quality audits
- Investigating data integrity issues
- Validating de-duplication logic
- Pre-deployment validation

### Sample Code

```python
from pyspark.sql.functions import col, count
from pyspark.sql.window import Window

df = spark.sql("SELECT * FROM inc_fardap_lakehouse.fardap_silver_incidents")

# Find duplicates
window_spec = Window.partitionBy('content_identifier_frsincidentnumber')
df_duplicates = df.withColumn('cnt', count('*').over(window_spec)) \
                  .filter(col('cnt') > 1) \
                  .drop('cnt')

display(df_duplicates)
```

### Expected Results

- **No duplicates:** Empty result (good!)
- **Duplicates found:** Review documentId values and timestamps to understand why

---

## Quick Reference

| Task | Notebook |
|:-----|:---------|
| Find your FRS ID | `Find_Your_FRS_ID` |
| Understand data model | `Explore_Package_Schema` |
| Browse valid values / lookups | `Explore_Controlled_Lists` |
| Build field mappings | `FaRDaP_Schema_Reference_Data` |
| Debug raw data | `RawJson_explorer` |
| Investigate incident | `Incident_Deep_Dive` |
| Check for duplicates | `dup_frsincidentnumber` |

---

[← Back to README](../README.md) | [Configuration →](CONFIGURATION.md)
