# Configuration Guide

> Environment setup and variable configuration for the FaRDaP Analytical Fabric Ingestion Platform

---

## Table of Contents

- [Variable Library](#variable-library)
- [Environment Configuration](#environment-configuration)
- [Azure Key Vault](#azure-key-vault)
- [FRS ID Lookup](#frs-id-lookup)
- [Performance Tuning](#performance-tuning)
- [Change Data Capture (CDC) Configuration](#change-data-capture-cdc-configuration)

---

## Variable Library

All notebooks use a shared **Fabric Variable Library** (`var_library_fardap`) for configuration. This enables environment-specific deployments without code changes.

### Base Variables

Located in `var_library_fardap.VariableLibrary/variables.json`:

| Variable | Type | Description | Required |
|:---------|:-----|:------------|:---------|
| `API_BASE_URL` | String | FaRDaP API endpoint | ✅ Yes |
| `FRS_ID` | String | Fire & Rescue Service identifier | ✅ Yes |
| `LAKEHOUSE_NAME` | String | Target Fabric Lakehouse name | ✅ Yes |
| `KEY_VAULT_URI` | String | Azure Key Vault URI | ✅ Yes |
| `CDC_DESCRIPTION_MODE` | String | Change description format: `Compact`, `Detailed`, or `Complete` | ✅ Yes (Default: `Detailed`) |
| `TABLE_FULL` | String | Bronze full table name | Optional |
| `TABLE_CDC` | String | CDC log table name | Optional |

### Example: variables.json

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/variables/1.0.0/schema.json",
  "variables": [
    {
      "name": "API_BASE_URL",
      "type": "String",
      "value": ""
    },
    {
      "name": "FRS_ID",
      "type": "String",
      "value": "17"
    },
    {
      "name": "LAKEHOUSE_NAME",
      "type": "String",
      "value": ""
    },
    {
      "name": "KEY_VAULT_URI",
      "type": "String",
      "value": ""
    },
    {
      "name": "CDC_DESCRIPTION_MODE",
      "type": "String",
      "value": "Detailed"
    }
  ]
}
```

### Accessing Variables in Notebooks

```python
# Get the Variable library
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")

# Read variables
API_BASE_URL = vl.getVariable("API_BASE_URL")
FRS_ID = vl.getVariable("FRS_ID")
LAKEHOUSE_NAME = vl.getVariable("LAKEHOUSE_NAME")
KEY_VAULT_URI = vl.getVariable("KEY_VAULT_URI")
CDC_DESCRIPTION_MODE = vl.getVariable("CDC_DESCRIPTION_MODE", "Detailed")  # Default to Detailed
```

---

## Environment Configuration

The Variable Library supports **environment-specific overrides** through value sets.

### Development Environment

Located in `var_library_fardap.VariableLibrary/valueSets/dev.json`:

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/valueSet/1.0.0/schema.json",
  "name": "dev",
  "variableOverrides": [
    {
      "name": "API_BASE_URL",
      "value": "https://www.fardap-training.fire.gov.uk"
    },
    {
      "name": "LAKEHOUSE_NAME",
      "value": "fardap_lakehouse_dev"
    }
  ]
}
```

### Production Environment

Located in `var_library_fardap.VariableLibrary/valueSets/prod.json`:

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/valueSet/1.0.0/schema.json",
  "name": "prod",
  "variableOverrides": [
    {
      "name": "API_BASE_URL",
      "value": "https://www.fardap.fire.gov.uk"
    },
    {
      "name": "LAKEHOUSE_NAME",
      "value": "fardap_lakehouse"
    },
    {
      "name": "KEY_VAULT_URI",
      "value": "https://devfardap.vault.azure.net/"
    }
  ]
}
```

### Environment Comparison

| Variable | Development | Production |
|:---------|:------------|:-----------|
| `API_BASE_URL` | `https://www.fardap-training.fire.gov.uk` | `https://www.fardap.fire.gov.uk` |
| `LAKEHOUSE_NAME` | `fardap_lakehouse_dev` | `fardap_lakehouse` |
| `KEY_VAULT_URI` | (inherited) | `https://devfardap.vault.azure.net/` |

---

## Azure Key Vault

API credentials are stored securely in **Azure Key Vault** and retrieved at runtime.

### Required Secrets

| Secret Name | Description |
|:------------|:------------|
| `FARDAP-API-USERNAME` | API authentication username |
| `FARDAP-API-PASSWORD` | API authentication password |

### Accessing Secrets in Notebooks

```python
# Get Key Vault URI from Variable Library
KEY_VAULT_URI = vl.getVariable("KEY_VAULT_URI")

# Retrieve secrets
USERNAME = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-USERNAME")
PASSWORD = notebookutils.credentials.getSecret(KEY_VAULT_URI, "FARDAP-API-PASSWORD")
```

### Setting Up Key Vault Access

1. **Create Azure Key Vault** (if not exists)
2. **Add secrets:**
   - `FARDAP-API-USERNAME`: Your FaRDaP username
   - `FARDAP-API-PASSWORD`: Your FaRDaP password
3. **Grant access** to Fabric workspace managed identity:
   - Key Vault → Access policies → Add access policy
   - Secret permissions: Get, List
   - Select principal: Your Fabric workspace identity

> ⚠️ **Security Note:** Never hardcode credentials in notebooks or source control.

---

## FRS ID Lookup

The **FRS ID** is a numeric identifier for your Fire and Rescue Service organisation.

### Important

> ⚠️ Use the **numeric FRS ID**, NOT the two-character IRS code (HS, GM, WY).

### Common FRS IDs

| Fire & Rescue Service | FRS ID | IRS Code (DON'T USE) |
|:----------------------|:-------|:---------------------|
| London South | 39 | LS |
| Greater Manchester | 28 | GM |
| West Yorkshire | 42 | WY |
| Merseyside | 33 | MY |
| West Midlands | 41 | WM |
| South Yorkshire | 38 | SY |

### Finding Your FRS ID

Run the `Find_Your_FRS_ID.Notebook` to:

1. Authenticate with FaRDaP API
2. Fetch `FRSIdListType` Reference Data
3. Display all available FRS organisations
4. Test your FRS ID

### Verification

After setting your FRS ID, verify by running:

```python
# In any notebook after configuration
vl = notebookutils.variableLibrary.getLibrary("var_library_fardap")
FRS_ID = vl.getVariable("FRS_ID")
print(f"Configured FRS ID: {FRS_ID}")
```

---

## Performance Tuning

### Default Parameters

| Parameter | Default | Description |
|:----------|:--------|:------------|
| `BATCH_SIZE` | 1,000 | Records per API search page |
| `MAX_WORKERS` | 32 | Parallel fetch threads |
| `MAX_ATTEMPTS` | 5 | Retry attempts for failed requests |
| `BASE_BACKOFF` | 0.5s | Initial retry delay (exponential) |
| `REFRESH_EVERY` | 25,000 | Re-authenticate after N fetches |

### Adjusting Performance

These parameters are defined in the notebook code and can be modified:

```python
# In Bronze notebooks
BATCH_SIZE = 1000      # Decrease if API rate limiting occurs
MAX_WORKERS = 32        # Decrease if getting 429 errors
MAX_ATTEMPTS = 5        # Increase for unreliable networks
BASE_BACKOFF = 0.5      # Increase if frequent retries needed
REFRESH_EVERY = 25000   # Count-based token refresh (backup mechanism)
```

**Authentication Notes:**
- **Time-based refresh:** Primary mechanism, automatically refreshes when < 5 minutes remaining
- **Count-based refresh:** Backup mechanism defined by `REFRESH_EVERY`
- **Belt-and-suspenders:** Both mechanisms run independently for maximum reliability
- Token expiry tracked from API response (typically 3600 seconds / 1 hour)

### Performance Recommendations

| Scenario | Adjustment |
|:---------|:-----------|
| Getting 429 (Rate Limited) errors | Reduce `MAX_WORKERS` to 16 or 8 |
| Slow API responses | Reduce `BATCH_SIZE` to 500 |
| Frequent timeouts | Increase `MAX_ATTEMPTS` to 10 |
| Token expiration issues | Time-based refresh should auto-handle; check logs |
| Fast, reliable network | Increase `MAX_WORKERS` to 64 |

### Expected Performance Metrics

| Metric | Expected Value |
|:-------|:---------------|
| Bronze Incremental Sync | 1-2 minutes |
| Silver Incremental Transform | < 30 seconds |
| Content hash skip rate | 80-95% |
| Full pipeline (incremental) | < 2 minutes |

---

## Change Data Capture (CDC) Configuration

The platform provides **configurable change tracking** with three description modes. Configure via the `CDC_DESCRIPTION_MODE` variable.

### Description Modes

| Mode | Description | Storage Impact | Performance Overhead |
|:-----|:------------|:---------------|:---------------------|
| **Compact** | Field names only | Small (~150 bytes/update) | 2-3% |
| **Detailed** | First 5 fields with old→new values (recommended) | Medium (~750 bytes/update) | 3-5% |
| **Complete** | Full JSON of all changes | Large (~5 KB/update) | 5-8% |

### Configuration

Add to `variables.json`:

```json
{
  "name": "CDC_DESCRIPTION_MODE",
  "type": "String",
  "value": "Detailed",
  "note": "Change description format: Compact, Detailed, or Complete"
}
```

### Environment-Specific CDC Modes

Override per environment in `valueSets/dev.json` or `valueSets/prod.json`:

```json
{
  "name": "dev",
  "variableOverrides": [
    {
      "name": "CDC_DESCRIPTION_MODE",
      "value": "Detailed"
    }
  ]
}
```

### Example Outputs

**Compact:**
```
5 fields changed: content_status, content_priority, content_location, content_severity, content_assignedto
```

**Detailed (Recommended):**
```
content_status: 'Open' → 'Closed'; content_priority: 'High' → 'Medium'; +3 other fields
```

**Complete:**
```json
{"content_status": {"old": "Open", "new": "Closed"}, "content_priority": {"old": "High", "new": "Medium"}}
```

### Important Limitations

⚠️ **CDC tracks changes AFTER initial load only:**

- ✅ Captures all changes from first incremental pipeline run forward
- ❌ Does NOT reconstruct historical changes from before deployment
- ❌ Rebuilding Silver layer clears CDC history

**Best Practice:** Archive `fardap_silver_cdc_log` before rebuilding Silver layer to preserve change history.

For complete CDC documentation, see [CDC Change Tracking Guide](CDC_CHANGE_TRACKING.md).

---

## Checklist

Before running the pipelines, verify:

- [ ] Variable Library is configured with correct values
- [ ] Key Vault contains API credentials
- [ ] Fabric workspace has access to Key Vault
- [ ] FRS ID is set to the correct numeric value
- [ ] Lakehouse exists in the workspace
- [ ] Development vs Production environment is correct

---

[← Back to README](../README.md) | [Technical Documentation →](TECHNICAL_DOCUMENTATION.md)
