# FaRDaP Analytical Platform

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft-Fabric-blue)](https://www.microsoft.com/en-us/microsoft-fabric)
[![Power BI](https://img.shields.io/badge/Power%20BI-Direct%20Lake-yellow)](https://powerbi.microsoft.com/)
[![License](https://img.shields.io/badge/License-See%20LICENSE-green)](../LICENSE)

> **Enterprise data pipeline for Fire and Rescue Service incident data using Microsoft Fabric's Medallion Architecture**

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Documentation](#documentation)
- [Notebooks](#notebooks)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## Overview

The **FaRDaP Analytical Platform** is a Microsoft Fabric-based enterprise data pipeline that:

- 🔄 Extracts Fire and Rescue Service incident data from the **FaRDaP™ API**
- 💎 Stores data in a Lakehouse using the **Medallion Architecture** (Bronze → Silver)
- 📊 Enables analytical reporting through **Power BI Direct Lake** semantic models
- ⏱️ Runs **every 5 minutes** for near-real-time data

### What is FaRDaP?

**FaRDaP** (Fire and Rescue Data Platform) is the UK Home Office system for collecting and managing incident data from Fire and Rescue Services.

### Key Features

| Feature | Description |
|---------|-------------|
| 🔌 **API-Driven Ingestion** | Fetches incident documents from FaRDaP REST API |
| 🏅 **Medallion Architecture** | Bronze (raw), Silver (transformed) |
| ⚡ **Incremental Processing** | Content-hash-based change detection for efficiency |
| 🔮 **Dynamic Schema Discovery** | Automatically adapts to new fields/arrays |
| 🔄 **Idempotent Operations** | Safe to re-run without data corruption |
| 📋 **Change Data Capture** | Detailed audit trail with 3 description modes (tracks changes after initial load) |
| 🔐 **Smart Token Management** | Time-based + count-based refresh prevents auth failures |
| 🌐 **API Compliance** | User-Agent header with FRS identification per FaRDaP spec |
| 🌍 **Environment Support** | Development and Production configurations |
| 🔗 **Azure DevOps Integration** | Git-based source control and CI/CD |

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   FaRDaP API    │────▶│  BRONZE LAYER   │────▶│  SILVER LAYER   │
│  (REST Service) │     │   (Raw JSON)    │     │ (Normalized)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │                        │
                               ▼                        ▼
                        ┌─────────────┐          ┌─────────────┐
                        │  CDC Log    │          │  Power BI   │
                        │  Sync State │          │Direct Lake  │
                        └─────────────┘          └─────────────┘
```

### Layer Responsibilities

| Layer | Purpose | Data Format | Update Frequency |
|:------|:--------|:------------|:-----------------|
| **Bronze** | Raw data preservation | JSON documents | Every 5 minutes |
| **Silver** | Business-ready data | Flattened columns + normalized arrays | After Bronze sync |
| **Semantic Model** | Power BI reporting | Direct Lake connection | Real-time |

---

## Quick Start

### Prerequisites

- Microsoft Fabric workspace with Lakehouse
- Azure Key Vault with API credentials
- FaRDaP API access (username/password)
- Your FRS ID (Fire & Rescue Service identifier)

### Initial Setup

```bash
# 1. Configure Variable Library (dev or prod values)
# 2. Add secrets to Azure Key Vault
#    - FARDAP-API-USERNAME
#    - FARDAP-API-PASSWORD

# 3. Run Find_Your_FRS_ID notebook to verify your FRS ID

# 4. Execute the full load pipeline
#    PL_FaRDaP_inc_full_load

# 5. Enable the scheduled incremental pipeline
#    PL_FaRDaP_inc_incremental (runs every 5 minutes)

# 6. Connect Power BI to the Semantic Model
```

### Finding Your FRS ID

> ⚠️ **Important:** Use the numeric FRS ID, NOT the two-character IRS code.

| Service | FRS ID |
|:--------|:-------|
| London South | 39 |
| Greater Manchester | 28 |
| West Yorkshire | 42 |

Run the `Find_Your_FRS_ID.Notebook` to discover your organisation's ID.

---

## Documentation

| Document | Description |
|:---------|:------------|
| [📖 Technical Documentation](guideance/TECHNICAL_DOCUMENTATION.md) | Complete technical reference |
| [🔧 Configuration Guide](guideance/CONFIGURATION.md) | Environment setup and variables |
| [📊 Data Pipeline Guide](guideance/DATA_PIPELINES.md) | Pipeline execution and scheduling |
| [� CDC Change Tracking](guideance/CDC_CHANGE_TRACKING.md) | Change data capture and audit trail |
| [�🗄️ Table Reference](guideance/TABLE_REFERENCE.md) | All Bronze and Silver tables |
| [🛠️ Supporting Scripts](guideance/SUPPORTING_SCRIPTS.md) | Utility notebooks for exploration |

---

## Notebooks

### Core Pipeline Notebooks

| Notebook | Layer | Purpose | Duration |
|:---------|:------|:--------|:---------|
| `01_Bronze_Full_Load` | Bronze | Full extraction of all incidents | 30 min - hours |
| `01_Bronze_Incremental_Sync` | Bronze | Fetch changed documents only | 1-2 min |
| `02_Silver_Full_Transform_Enhanced` | Silver | Transform all Bronze to Silver | 10-30 min |
| `02_Silver_Incremental_Transform_Enhanced` | Silver | Transform changed records only | Seconds |

### Data Pipelines

| Pipeline | Purpose | Schedule |
|:---------|:--------|:---------|
| `PL_FaRDaP_inc_full_load` | Initial setup / disaster recovery | Manual |
| `PL_FaRDaP_inc_incremental` | Regular incremental updates | Every 5 minutes |

### Supporting Scripts

| Notebook | Purpose |
|:---------|:--------|
| `00_Explore_Package_Schema` | Discover FaRDaP API schema |
| `Find_Your_FRS_ID` | Look up your FRS identifier |
| `Explore_Controlled_Lists` | Browse valid values / reference data |
| `FaRDaP_Schema_Reference_Data` | Build field mappings for reports |
| `00_RawJson_explorer` | Explore raw JSON structure |
| `Incident_Deep_Dive` | Deep dive into single incident |
| `dup_frsincidentnumber` | Find duplicate incident numbers |

See [Supporting Scripts Guide](guideance/SUPPORTING_SCRIPTS.md) for detailed documentation.

---

## Configuration

### Variable Library

All notebooks use `var_library_fardap` for configuration:

```json
{
  "variables": [
    { "name": "API_BASE_URL", "type": "String" },
    { "name": "FRS_ID", "type": "String" },
    { "name": "LAKEHOUSE_NAME", "type": "String" },
    { "name": "KEY_VAULT_URI", "type": "String" },
    { "name": "CDC_DESCRIPTION_MODE", "type": "String", "value": "Detailed" }
  ]
}
```

### Environment Overrides

<details>
<summary><b>Development (dev.json)</b></summary>

```json
{
  "name": "dev",
  "variableOverrides": [
    { "name": "API_BASE_URL", "value": "https://www.fardap-training.fire.gov.uk" },
    { "name": "LAKEHOUSE_NAME", "value": "fardap_lakehouse_dev" }
  ]
}
```

</details>

<details>
<summary><b>Production (prod.json)</b></summary>

```json
{
  "name": "prod",
  "variableOverrides": [
    { "name": "API_BASE_URL", "value": "https://www.fardap.fire.gov.uk" },
    { "name": "LAKEHOUSE_NAME", "value": "fardap_lakehouse" },
    { "name": "KEY_VAULT_URI", "value": "https://devfardap.vault.azure.net/" }
  ]
}
```

</details>

### Performance Tuning

| Parameter | Default | Description |
|:----------|:--------|:------------|
| `BATCH_SIZE` | 1,000 | Records per API page (server caps at 1,000 per FaRDaP spec) |
| `MAX_WORKERS` | 32 | Parallel fetch threads |
| `MAX_ATTEMPTS` | 5 | Retry attempts |
| `BASE_BACKOFF` | 0.5s | Initial retry delay |
| `REFRESH_EVERY` | 25,000 | Count-based token refresh interval |

### Authentication Features

FaRDaP access tokens have a short lifetime (~20 minutes per spec). The platform uses the dedicated refresh-token endpoint to keep sessions alive without re-sending credentials.

| Feature | Implementation |
|:--------|:---------------|
| **Token Expiry Tracking** | Captures `expiresIn` from API; conservative 600s default if absent |
| **Refresh-Token Flow** | Calls `/api/v1/auth/access-token-refresh` with stored `refreshToken` |
| **Proactive Refresh** | Refreshes when < 2 minutes remaining (suited to ~20-minute tokens) |
| **Count-Based Refresh** | Refreshes every 25,000 documents (belt-and-suspenders) |
| **User-Agent Header** | `Fabric/FaRDaP-Analytical-Platform/FRS-{FRS_ID}` on all API requests |
| **Thread-Safe Updates** | Token updates protected by lock for parallel processing |
| **Fallback to Re-auth** | If refresh fails, transparently re-authenticates via `/auth/init` |

### Security

All FaRDaP API calls use HTTPS with **certificate verification enabled** (no `verify=False`). If your environment uses a private/self-signed CA, set `REQUESTS_CA_BUNDLE` to the CA bundle path on the Spark workers rather than disabling verification.

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|:------|:------|:---------|
| Authentication Failed (401) | Invalid credentials | Check Key Vault secrets |
| Token Expired Mid-Process | Long-running job | Should auto-refresh; check logs for "Token expiring soon" |
| Rate Limited (429) | Too many API requests | Reduce `MAX_WORKERS` |
| "State table not found" | Prerequisites missing | Run Full Load first |
| "No documents retrieved" | Wrong FRS_ID | Verify FRS_ID and permissions |
| Schema mismatch | Column changes | Run Full Transform to reset |

### Performance Issues

| Symptom | Investigation | Solution |
|:--------|:--------------|:---------|
| Slow Bronze sync | API response times | Reduce `BATCH_SIZE` |
| Silver transform slow | Large JSON changes | Normal for initial loads |
| Power BI not updating | Direct Lake cache | Refresh semantic model |
| High skip rate in Silver | Content unchanged | Expected behaviour (good!) |

### Data Quality Checks

| Check | Tool |
|:------|:-----|
| Duplicate incident numbers | `dup_frsincidentnumber.Notebook` |
| Valid field values | `Explore_Controlled_Lists.Notebook` |
| Raw data inspection | `RawJson_explorer.Notebook` |
| Single incident analysis | `Incident_Deep_Dive.Notebook` |

---

## Workspace Structure

```
inc_fardap_analytical_platform/
├── inc_fardap_lakehouse.Lakehouse/      # Delta Lake storage
├── var_library_fardap.VariableLibrary/  # Environment config
│   ├── variables.json
│   └── valueSets/
│       ├── dev.json
│       └── prod.json
├── Notebooks/
│   ├── 01_Bronze_Full_Load.Notebook/
│   ├── 01_Bronze_Incremental_Sync.Notebook/
│   ├── 02_Silver_Full_Transform_Enhanced.Notebook/
│   ├── 02_Silver_Incremental_Transform_Enhanced.Notebook/
│   ├── PL_FaRDaP_inc_full_load.DataPipeline/
│   ├── PL_FaRDaP_inc_incremental.DataPipeline/
│   └── Supporting_Scripts/
│       ├── 00_Explore_Package_Schema.Notebook/
│       ├── 00_RawJson_explorer.Notebook/
│       ├── Explore_Controlled_Lists.Notebook/
│       ├── FaRDaP_Schema_Reference_Data.Notebook/
│       ├── Find_Your_FRS_ID.Notebook/
│       ├── Incident_Deep_Dive.Notebook/
│       └── dup_frsincidentnumber.Notebook/
└── inc_fardap.SemanticModel/            # Power BI Direct Lake model
    └── definition/
        ├── database.tmdl
        ├── model.tmdl
        ├── relationships.tmdl
        └── tables/                      # 15 table definitions
```

---

## Contributing

1. Create a feature branch from `main`
2. Make your changes
3. Test with both full load and incremental pipelines
4. Submit a pull request

---

## License

See [LICENSE](../LICENSE) for details.

---

<p align="center">
  <sub>Built with ❤️ using Microsoft Fabric</sub>
</p>
