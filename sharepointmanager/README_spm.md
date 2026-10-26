# SharePointManager

Production-ready SharePoint integration for the **gdap_spark** project.  
Supports downloading files/folders from SharePoint into a Microsoft Fabric Lakehouse and uploading Spark DataFrames or Delta tables (including Excel) back to SharePoint.

## Features

- Download single file or entire folder (recursive) → Lakehouse `Files`
- Upload DataFrame or Delta table as **CSV**, **Parquet**, or **Excel (.xlsx)**
- Large-file support via Microsoft Graph resumable upload sessions
- Folder listing
- Retry with exponential backoff
- Uses existing **Service Principal + Azure Key Vault**
- Integrates with project logging via `GDAPLogging` (`gap_logging`)
- Ingest-style and Export-style helper methods

## Prerequisites

### Azure / SharePoint
- Service Principal with SharePoint / Microsoft Graph permissions  
  (`Sites.ReadWrite.All` or more granular)
- Secrets stored in Azure Key Vault:
  - `sp-client-id`
  - `sp-client-secret`
  - `sp-tenant-id`
  - `sharepoint-site-id` (recommended)

### Fabric / Environment
- Microsoft Fabric workspace with a Lakehouse
- Notebook attached to the Lakehouse
- Python packages:
  ```bash
  %pip install requests azure-identity azure-keyvault-secrets openpyxl pandas
