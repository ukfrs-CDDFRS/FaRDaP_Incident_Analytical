# Data Pipelines

> Orchestration of the FaRDaP Analytical Fabric Ingestion Platform workflows

---

## Table of Contents

- [Overview](#overview)
- [Full Load Pipeline](#full-load-pipeline)
- [Incremental Pipeline](#incremental-pipeline)
- [Pipeline Dependencies](#pipeline-dependencies)
- [Scheduling](#scheduling)
- [Monitoring](#monitoring)

---

## Overview

The FaRDaP Analytical Platform uses **Microsoft Fabric Data Pipelines** to orchestrate notebook execution. Two pipelines support different scenarios:

| Pipeline | Purpose | Frequency |
|:---------|:--------|:----------|
| `PL_FaRDaP_inc_full_load` | Initial data load | On-demand |
| `PL_FaRDaP_inc_incremental` | Keep data current | Every 5 minutes |

### Execution Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PL_FaRDaP_inc_full_load                          │
├─────────────────────────────────────────────────────────────────────┤
│  01_Bronze_Full_Load  →  02_Silver_Full                             │
│      (parallel)           (transform)                               │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                  PL_FaRDaP_inc_incremental                          │
├─────────────────────────────────────────────────────────────────────┤
│  01_Bronze_Incremental  →  02_Silver_Incr                           │
│      (sync)                 (transform)                             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Full Load Pipeline

**Pipeline:** `PL_FaRDaP_inc_full_load`

**Trigger:** Manual (on-demand)

### Purpose

Complete data extraction and transformation for:
- Initial system setup
- Full refresh after schema changes
- Disaster recovery
- Environment provisioning

### Notebooks

1. `01_Bronze_Full_Load.Notebook` ⚡ **Dual-search enabled**
   - Searches both `territoryFrsId=17` AND `responsibleFrsId=17`
   - Deduplicates IDs before fetching (~132,700 unique incidents)
   - Initializes both watermarks in `fardap_sync_state`
2. `02_Silver_Full_Transform_Enhanced.Notebook`

**Expected Duration:** 30 minutes - 2 hours (depending on data volume)

**When to Run:**
- Initial platform setup
- After major schema changes
- Disaster recovery / data corruption
- Testing dual-search implementation

**Key Outputs:**
- `fardap_bronze_incidents`: All incidents (overwrites existing)
- `fardap_bronze_cdc_log`: Change records (appends)
- `fardap_sync_state`: Both watermarks initialized to same max timestamp

### Activities

```json
{
  "activities": [
    {
      "name": "Full Bronze Load",
      "type": "TridentNotebook",
      "notebook": "01_Bronze_Full_Load"
    },
    {
      "name": "Full Silver Transform",
      "type": "TridentNotebook",
      "notebook": "02_Silver_Full_Transform_Enhanced",
      "dependsOn": ["Full Bronze Load"]
    }
  ]
}
```

### Execution Order

1. **Bronze Full Load** (`01_Bronze_Full_Load`)
   - Fetch all IRS documents from API
   - Count records and plan parallel fetch
   - Write to `fardap_bronze_incidents` table (OVERWRITE mode)
   - Update watermark in `fardap_sync_state`

2. **Silver Full Transform** (`02_Silver_Full_Transform_Enhanced`)
   - Read entire bronze table
   - Flatten all nested JSON structures
   - Write to 11 silver tables (OVERWRITE mode)

### Expected Duration

| Step | Duration | Depends On |
|:-----|:---------|:-----------|
| Bronze Full Load | 10-30 minutes | Dataset size |
| Silver Transform | 3-10 minutes | Row count |
| **Total** | **15-40 minutes** | |

---

## Incremental Pipeline

**Pipeline:** `PL_FaRDaP_inc_incremental`

**Trigger:** Scheduled (every 5 minutes)

### Purpose

Keep data current with minimal processing:
- Near real-time updates (5-minute intervals)
- Only process changed records
- Minimal compute usage

### Notebooks

1. `01_Bronze_Incremental_Sync.Notebook` ⚡ **Dual-search with independent watermarks**
   - Reads `last_watermark_territory` and `last_watermark_responsible`
   - Executes two searches with different `dateUpdated >=` filters
   - Deduplicates overlapping IDs before fetching
   - Updates each watermark independently based on its search results
2. `02_Silver_Incremental_Transform_Enhanced.Notebook`

**Expected Duration:** 1-2 minutes (normal operations)

**Dual-Search Behavior:**

```
Example run:
  Previous watermarks: territory=08:05, responsible=08:10
  
  Search 1 (territory >= 08:05): Returns [incident_A, incident_B]
  Search 2 (responsible >= 08:10): Returns [incident_B, incident_C]
  
  Unique IDs to fetch: [A, B, C] (B fetched once, not twice!)
  
  New watermarks:
    territory: max(A.dateUpdated, B.dateUpdated)
    responsible: max(B.dateUpdated, C.dateUpdated)
```

**Performance:**
- Typically 0-50 changed incidents per run
- Cross-border incidents (~250-700 total) may update independently
- Deduplication prevents double-fetching when same incident appears in both searches

### Activities

```json
{
  "activities": [
    {
      "name": "Incremental Bronze Sync",
      "type": "TridentNotebook",
      "notebook": "01_Bronze_Incremental_Sync"
    },
    {
      "name": "Incremental Silver Transform",
      "type": "TridentNotebook",
      "notebook": "02_Silver_Incremental_Transform_Enhanced",
      "dependsOn": ["Incremental Bronze Sync"]
    }
  ]
}
```

### Schedule Configuration

```json
{
  "triggers": [
    {
      "type": "ScheduleTrigger",
      "recurrence": {
        "frequency": "Minute",
        "interval": 5
      }
    }
  ]
}
```

### Execution Order

1. **Bronze Incremental Sync** (`01_Bronze_Incremental_Sync`)
   - Read watermark from previous run
   - Fetch only records modified since watermark
   - MERGE into `fardap_bronze_incidents` table
   - Update watermark

2. **Silver Incremental Transform** (`02_Silver_Incremental_Transform_Enhanced`)
   - Read changed records from CDC log
   - Filter using content hash comparison
   - MERGE only actually-changed documents into silver tables

### Expected Duration

| Step | Duration | Typical Rows |
|:-----|:---------|:-------------|
| Bronze Sync | 30-90 seconds | 0-500 |
| Silver Transform | 5-30 seconds | 0-100 (after hash filtering) |
| **Total** | **< 2 minutes** | |

---

## Pipeline Dependencies

### Activity Dependencies

```
                Full Load Pipeline
                ═══════════════════
                         │
         ┌───────────────┴───────────────┐
         ▼                               │
   ┌──────────┐                          │
   │  Bronze  │                          │
   │   Full   │                          │
   │   Load   │                          │
   └────┬─────┘                          │
        │                                │
        ▼                                │
   ┌──────────┐                          │
   │  Silver  │◄─────────────────────────┘
   │   Full   │
   │Transform │
   └──────────┘
```

### Table Dependencies

| Layer | Tables Written | Depends On |
|:------|:---------------|:-----------|
| Bronze | `fardap_bronze_incidents`, `fardap_sync_state`, `fardap_bronze_cdc_log` | FaRDaP API |
| Silver | 11 tables (`fardap_silver_incidents`, `fardap_silver_victim`, etc.) | `fardap_bronze_incidents`, `fardap_bronze_cdc_log` |

---

## Scheduling

### Recommended Schedule

| Pipeline | Frequency | Notes |
|:---------|:----------|:------|
| Full Load | On-demand | Initial setup only |
| Incremental | Every 5 minutes | Production schedule |

### Adjusting Schedule

<details>
<summary>Changing Incremental Frequency</summary>

In Fabric:
1. Open `PL_FaRDaP_inc_incremental` pipeline
2. Click **Schedule**
3. Modify recurrence interval
4. Save and publish

Considerations:
- **1 minute:** Near real-time, higher API load
- **5 minutes:** Balanced (recommended)
- **15 minutes:** Lower cost, acceptable delay
- **1 hour:** Batch-style updates

</details>

### Schedule Best Practices

- ✅ Use 5-minute intervals for near real-time
- ✅ Disable schedule during full load
- ✅ Monitor for pipeline overlap
- ⚠️ Avoid sub-minute scheduling (API rate limits)
- ⚠️ Consider FaRDaP API maintenance windows

---

## Monitoring

### Pipeline Run Status

In Fabric workspace:
1. Navigate to **Monitoring Hub**
2. Filter by Pipeline type
3. View run history and duration

### Key Metrics to Monitor

| Metric | Normal Range | Alert If |
|:-------|:-------------|:---------|
| Bronze Sync Duration | 30-90 seconds | > 5 minutes |
| Silver Transform Duration | 5-30 seconds | > 2 minutes |
| Records Synced (Incremental) | 0-500 | > 5,000 (unusual spike) |
| Content Hash Skip Rate | 80-95% | < 50% |

### Failure Handling

| Failure Point | Impact | Resolution |
|:--------------|:-------|:-----------|
| Bronze API Error | No new data | Check API credentials, retry |
| Silver Transform Error | Stale silver tables | Check bronze data quality |

### Alerting Setup

Configure Fabric alerts for:
- Pipeline failures
- Duration exceeding threshold
- Consecutive run failures

---

## Quick Reference

### Running Full Load

```
1. Ensure incremental pipeline is paused
2. Run PL_FaRDaP_inc_full_load manually
3. Wait for completion (15-45 minutes)
4. Resume incremental pipeline schedule
```

### Running Incremental Manually

```
1. Navigate to PL_FaRDaP_inc_incremental
2. Click "Run Now"
3. Monitor in Monitoring Hub
4. Typical completion: < 3 minutes
```

### Viewing Pipeline History

```
1. Workspace → Monitoring Hub
2. Filter: Item Type = Data Pipeline
3. Select pipeline run
4. View activity details
```

---

[← Back to README](../README.md) | [Configuration →](CONFIGURATION.md)
