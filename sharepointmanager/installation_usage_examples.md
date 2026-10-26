gdap_spark/
├── sharepoint/
│   ├── __init__.py
│   └── sharepoint_manager.py

from gap_logging import GDAPLogging
from your_package.sharepoint_manager import SharePointManager

logger = GDAPLogging.get_logger("my_pipeline")

spm = SharePointManager(
    key_vault_url="https://your-kv.vault.azure.net/",
    site_id="yourdomain.sharepoint.com,xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx,yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
)

# Download Excel
spm.download_file(
    sharepoint_path="/sites/MySite/Shared Documents/reports/monthly.xlsx",
    lakehouse_target_path="Incoming/monthly.xlsx"
)

# Upload DataFrame as Excel
spm.upload_dataframe(
    df=df,
    sharepoint_folder_path="/sites/MySite/Shared Documents/exports",
    file_name="daily_summary.xlsx",
    format="excel",
    sheet_name="Summary"
)

# Export Delta table as Excel
spm.export_to_sharepoint(
    source="/lakehouse/default/Tables/orders",
    sharepoint_folder_path="/sites/MySite/Shared Documents/exports",
    file_name="orders_export.xlsx",
    format="excel"
)
