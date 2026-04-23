# Table Reference

> Complete schema reference for Bronze and Silver layer tables

---

## Table of Contents

- [Overview](#overview)
- [Bronze Layer Tables](#bronze-layer-tables)
- [Silver Layer Tables](#silver-layer-tables)
- [Silver Layer State Tables](#silver-layer-state-tables)
- [Semantic Model Relationships](#semantic-model-relationships)

---

## Overview

The FaRDaP Analytical Platform uses a **Medallion Architecture** with two data layers:

| Layer | Purpose | Tables | Update Mode |
|:------|:--------|:-------|:------------|
| 🥉 Bronze | Raw JSON storage | 3 | MERGE (incremental) / OVERWRITE (full) |
| 🥈 Silver | Normalized, typed | 11 + 4 state tables | MERGE (incremental) / OVERWRITE (full) |

---

## Bronze Layer Tables

### bronze_irs

**Description:** Raw IRS documents from FaRDaP API

| Column | Type | Description |
|:-------|:-----|:------------|
| `irs_urn` | String | Primary key - IRS Unique Reference Number |
| `raw_json` | String | Complete IRS document as JSON |
| `_metadata_api_modified` | Timestamp | API-reported last modification time |
| `_metadata_load_timestamp` | Timestamp | When record was loaded to Bronze |
| `_metadata_source` | String | Source identifier (e.g., "fardap_api") |
| `_metadata_content_hash` | String | MD5 hash of raw_json for change detection |

**Update Mode:** 
- Full Load: OVERWRITE
- Incremental: MERGE on `irs_urn`

---

### bronze_irs_watermark

**Description:** High-water mark for incremental sync

| Column | Type | Description |
|:-------|:-----|:------------|
| `watermark_id` | String | Always "latest" |
| `last_sync_timestamp` | Timestamp | When last sync completed |
| `last_api_modified_max` | Timestamp | Highest `modifiedDateTime` seen |
| `records_processed` | Long | Count of records in last sync |

**Update Mode:** OVERWRITE (single row)

---

### bronze_irs_cdc_log

**Description:** Change Data Capture log for incremental processing

| Column | Type | Description |
|:-------|:-----|:------------|
| `batch_id` | String | UUID for this sync batch |
| `irs_urn` | String | Document identifier |
| `operation` | String | "INSERT" or "UPDATE" |
| `old_content_hash` | String | Previous hash (NULL for INSERT) |
| `new_content_hash` | String | Current hash |
| `_metadata_api_modified` | Timestamp | API modification time |
| `_metadata_cdc_timestamp` | Timestamp | When CDC record was created |

**Update Mode:** APPEND (immutable log)

**Usage:** Silver layer reads this to determine which documents to process

---

## Silver Layer Tables

All Silver tables share a common primary key (`documentId`) that maps to the IRS document.

### incidents (Main Fact Table)

**Description:** Core incident records with flattened top-level fields

> ℹ️ **Note:** This table contains **100+ columns** dynamically flattened from the JSON structure. The columns below are key fields - see the actual table for full schema.

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | Int64 | Primary key (from IRS document ID) |
| `raw_json` | String | Complete source JSON (for reference) |
| `content_hash` | String | MD5 hash for change detection |
| `sync_timestamp` | DateTime | When synced from Bronze |
| `flattened_timestamp` | DateTime | When flattened to Silver |
| `processed_at` | DateTime | Processing timestamp |
| `content_identifier_frsincidentnumber` | String | Human-readable incident ID |
| `content_auditdetail_createdby` | String | Creator |
| `content_auditdetail_datecreated` | String | Creation date |
| `content_auditdetail_dateupdated` | String | Last update date |
| `content_*` | Various | 100+ flattened content fields |

**Relationships:** One-to-Many to all other Silver tables via `documentId`

**Dynamic Columns:** The Silver transform dynamically discovers and flattens all non-array fields from the JSON, prefixing them with `content_`. This means new API fields are automatically captured without code changes.

---

### victim

**Description:** Casualty/victim information per incident

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | String | Foreign key to incidents |
| `_array_index` | Integer | Position in source array |
| `victimType` | String | Type of victim |
| `gender` | String | Gender |
| `age` | Integer | Age |
| `severity` | String | Injury severity |
| `rescuedBy` | String | Rescue method |
| `_metadata_source_hash` | String | Bronze content hash |

**Cardinality:** Multiple victims per incident

---

### vehicle

**Description:** Vehicle involvement records

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | String | Foreign key to incidents |
| `_array_index` | Integer | Position in source array |
| `vehicleType` | String | Type of vehicle |
| `vehicleMake` | String | Manufacturer |
| `vehicleModel` | String | Model |
| `registrationYear` | Integer | Registration year |
| `_metadata_source_hash` | String | Bronze content hash |

**Cardinality:** Multiple vehicles per incident

---

### hazardousmaterial

**Description:** Hazardous materials involved

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | String | Foreign key to incidents |
| `_array_index` | Integer | Position in source array |
| `materialType` | String | Type of hazmat |
| `quantity` | Double | Amount |
| `unit` | String | Unit of measurement |
| `containerType` | String | Container description |
| `_metadata_source_hash` | String | Bronze content hash |

**Cardinality:** Multiple materials per incident

---

### equipment

**Description:** Equipment used during response

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | String | Foreign key to incidents |
| `_array_index` | Integer | Position in source array |
| `equipmentType` | String | Type of equipment |
| `quantity` | Integer | Count used |
| `_metadata_source_hash` | String | Bronze content hash |

**Cardinality:** Multiple equipment records per incident

---

### buildingfacility

**Description:** Building/facility information

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | String | Foreign key to incidents |
| `_array_index` | Integer | Position in source array |
| `facilityType` | String | Type of facility |
| `floors` | Integer | Number of floors |
| `occupancy` | String | Occupancy status |
| `_metadata_source_hash` | String | Bronze content hash |

**Cardinality:** Multiple facilities per incident

---

### system

**Description:** Fire detection/suppression systems

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | String | Foreign key to incidents |
| `_array_index` | Integer | Position in source array |
| `systemType` | String | Type of system |
| `operationalStatus` | String | System status |
| `activationStatus` | String | Whether activated |
| `_metadata_source_hash` | String | Bronze content hash |

**Cardinality:** Multiple systems per incident

---

### manualsystem

**Description:** Manual firefighting systems

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | String | Foreign key to incidents |
| `_array_index` | Integer | Position in source array |
| `systemType` | String | Type of manual system |
| `used` | Boolean | Whether used |
| `_metadata_source_hash` | String | Bronze content hash |

**Cardinality:** Multiple systems per incident

---

### additionalinfo

**Description:** Extended incident information

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | String | Foreign key to incidents |
| `_array_index` | Integer | Position in source array |
| `infoType` | String | Type of information |
| `infoValue` | String | Information value |
| `_metadata_source_hash` | String | Bronze content hash |

**Cardinality:** Multiple info records per incident

---

### qasummaries

**Description:** Quality assurance validation summaries

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | String | Foreign key to incidents |
| `_array_index` | Integer | Position in source array |
| `qaCategory` | String | QA category |
| `status` | String | Validation status |
| `_metadata_source_hash` | String | Bronze content hash |

**Cardinality:** Multiple QA records per incident

---

### validation

**Description:** Data validation results

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | String | Foreign key to incidents |
| `_array_index` | Integer | Position in source array |
| `validationRule` | String | Rule applied |
| `result` | String | Pass/Fail |
| `message` | String | Validation message |
| `_metadata_source_hash` | String | Bronze content hash |

**Cardinality:** Multiple validation records per incident

---

## Silver Layer State Tables

These tables track processing state and enable incremental updates.

### fardap_silver_content_hash

**Description:** Tracks content hashes to detect actual data changes (not just timestamp changes)

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | Int64 | Primary key - Document identifier |
| `content_hash` | String | MD5 hash of processed content |
| `last_flattened_at` | DateTime | When document was last processed |

**Purpose:** Enables content-hash-based filtering in Silver incremental - only re-process documents where content actually changed.

---

### fardap_silver_cdc_log

**Description:** Change Data Capture log for Silver layer transformations

| Column | Type | Description |
|:-------|:-----|:------------|
| `documentId` | Int64 | Document identifier |
| `op_type` | String | Operation type (INSERT/UPDATE) |
| `flattened_at` | DateTime | When flattening occurred |
| `cdc_timestamp` | DateTime | CDC record timestamp |

**Purpose:** Audit trail for Silver layer transformations.

---

### fardap_silver_flatten_state

**Description:** Tracks overall Silver layer processing state

| Column | Type | Description |
|:-------|:-----|:------------|
| `total_flattened` | Int64 | Total documents processed |
| `last_watermark` | String | Last processed watermark |
| `mode` | String | Processing mode (FULL/INCREMENTAL) |

**Purpose:** Enables resumable processing and state tracking.

---

### fardap_sync_state

**Description:** Bronze sync state watermark

| Column | Type | Description |
|:-------|:-----|:------------|
| `last_watermark` | String | Last sync watermark timestamp |

**Purpose:** Single-row table tracking the Bronze layer's last successful sync point.

---

## Semantic Model Relationships

The Power BI Direct Lake semantic model connects all Silver tables via `documentId`:

```
                           ┌──────────────┐
                           │   incidents  │
                           │   (fact)     │
                           │              │
                           │  documentId  │
                           └──────┬───────┘
                                  │
      ┌───────────────┬───────────┼───────────┬───────────────┐
      │               │           │           │               │
      ▼               ▼           ▼           ▼               ▼
┌──────────┐   ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│  victim  │   │ vehicle  │ │equipment │ │  system  │ │validation│
│          │   │          │ │          │ │          │ │          │
│documentId│   │documentId│ │documentId│ │documentId│ │documentId│
└──────────┘   └──────────┘ └──────────┘ └──────────┘ └──────────┘
      │               │           │           │               │
      └───────────────┴───────────┴───────────┴───────────────┘
                                  │
                    (additional dimension tables)
                                  │
      ┌───────────────┬───────────┼───────────┬───────────────┐
      ▼               ▼           ▼           ▼               ▼
┌────────────┐ ┌────────────┐ ┌─────────┐ ┌────────────┐ ┌─────────────┐
│ hazardous  │ │ building   │ │ manual  │ │ additional │ │ qasummaries │
│ material   │ │ facility   │ │ system  │ │    info    │ │             │
└────────────┘ └────────────┘ └─────────┘ └────────────┘ └─────────────┘
```

### Relationship Details

| From Table | To Table | Cardinality | Active |
|:-----------|:---------|:------------|:-------|
| incidents | victim | One-to-Many | Yes |
| incidents | vehicle | One-to-Many | Yes |
| incidents | equipment | One-to-Many | Yes |
| incidents | system | One-to-Many | Yes |
| incidents | manualsystem | One-to-Many | Yes |
| incidents | hazardousmaterial | One-to-Many | Yes |
| incidents | buildingfacility | One-to-Many | Yes |
| incidents | additionalinfo | One-to-Many | Yes |
| incidents | qasummaries | One-to-Many | Yes |
| incidents | validation | One-to-Many | Yes |

### Direct Lake Benefits

- **No data import needed** - queries run directly on Lakehouse Delta tables
- **Real-time updates** - changes visible after pipeline completion
- **Reduced storage** - no duplicate data in Power BI
- **High performance** - columnar storage with predicate pushdown

### Tables in Semantic Model

The semantic model includes all Silver entity tables plus state tracking tables:

| Table | Purpose |
|:------|:--------|
| `fardap_silver_incidents` | Main fact table (100+ columns) |
| `fardap_silver_victim` | Casualty records |
| `fardap_silver_vehicle` | Vehicle involvement |
| `fardap_silver_equipment` | Equipment used |
| `fardap_silver_system` | Fire detection systems |
| `fardap_silver_manualsystem` | Manual firefighting systems |
| `fardap_silver_hazardousmaterial` | Hazmat involvement |
| `fardap_silver_buildingfacility` | Building information |
| `fardap_silver_additionalinfo` | Extended info |
| `fardap_silver_qasummaries` | QA validation |
| `fardap_silver_validation` | Data validation results |
| `fardap_silver_cdc_log` | Change tracking |
| `fardap_silver_content_hash` | Content hash registry |
| `fardap_silver_flatten_state` | Processing state |
| `fardap_sync_state` | Sync watermark |

---

[← Back to README](../README.md) | [Technical Documentation →](TECHNICAL_DOCUMENTATION.md)
