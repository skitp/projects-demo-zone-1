import json
import re
import os
from typing import Dict, Optional, List
import logging
import fsspec
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
import notebookutils

from spark_engine.common.email_util import send_email
from spark_engine.common.lakehouse import LakehouseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION & CONSTANTS
# =============================================================================
# blobPath is set by the Activator / parameter cell, e.g.:
# blobPath = "/blobServices/default/containers/fnol-claims-pdfs/blobs/GT-5801d372.pdf"
blobPath = "/blobServices/default/containers/fnol-claims-pdfs/blobs/GT-5801d372.pdf"  # example / parameter

# Key Vault secret names for the Service Principal that has access to the storage account
# (Storage Blob Data Contributor or equivalent). Adjust names if they differ in your vault.
SP_CLIENT_ID_SECRET = "fnolai-storage-client-id"
SP_CLIENT_SECRET_SECRET = "fnolai-storage-client-secret"
SP_TENANT_ID_SECRET = "fnolai-storage-tenant-id"

# Other configuration
template_name = "fnolai_web_entry.json"
attachments_tmp_dir = "tmp"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def get_template_location_url(
    lakehouse_name: str = "den_lhw_pdi_001_metadata",
    notification_type: str = "emails",
    file_name: str = ""
) -> Optional[str]:
    """Get the ABFSS URL for the email template in the metadata lakehouse."""
    try:
        lakehouse_manager = LakehouseManager(lakehouse_name=lakehouse_name)
        template_path = f"{lakehouse_manager.lakehouse_path}/Files/templates/{notification_type}"
        return f"{template_path}/{file_name}"
    except Exception as e:
        logger.error(f"Failed to get template location URL: {str(e)}")
        return None


def replace_tokens_in_json_object(json_object: Dict, param_dict: Dict) -> Optional[Dict]:
    """Replace {token} placeholders in a JSON object with values from param_dict."""
    try:
        value = json.dumps(json_object)
        for k, v in param_dict.items():
            v = str(v) if v is not None else ""
            value = value.replace('{' + k + '}', v)
        return json.loads(value)
    except (json.JSONDecodeError, TypeError) as e:
        logger.error(f"Error replacing tokens in JSON: {str(e)}")
        return None


def read_json_file(file_location: str) -> Optional[Dict]:
    """Read and parse a JSON file from OneLake / ABFSS."""
    try:
        json_df = spark.read.text(file_location, wholetext=True)
        content = json_df.first()["value"]
        return json.loads(content)
    except (json.JSONDecodeError, Exception) as e:
        logger.error(f"Error reading JSON file {file_location}: {str(e)}")
        return None


def get_service_principal_credential(key_vault_name: str) -> ClientSecretCredential:
    """
    Retrieve Service Principal credentials from Key Vault and return a ClientSecretCredential.
    Secrets must already exist in the vault.
    """
    try:
        client_id = notebookutils.credentials.getSecret(key_vault_name, SP_CLIENT_ID_SECRET)
        client_secret = notebookutils.credentials.getSecret(key_vault_name, SP_CLIENT_SECRET_SECRET)
        tenant_id = notebookutils.credentials.getSecret(key_vault_name, SP_TENANT_ID_SECRET)

        if not all([client_id, client_secret, tenant_id]):
            raise ValueError("One or more Service Principal secrets returned empty values")

        logger.info("Successfully retrieved Service Principal credentials from Key Vault")
        return ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
    except Exception as e:
        logger.error(f"Failed to create Service Principal credential: {str(e)}")
        raise


def download_blob_to_onelake(
    storage_account_name: str,
    container_name: str,
    blob_name: str,
    credential: ClientSecretCredential,
    dest_path: str
) -> str:
    """
    Download a blob from Azure Storage using Service Principal auth
    and write it to OneLake (ABFSS).
    """
    account_url = f"https://{storage_account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    logger.info(f"Downloading blob '{blob_name}' from container '{container_name}'")
    pdf_bytes = blob_client.download_blob().readall()

    # Write binary data to ABFSS
    STORAGE_OPTIONS = {
        "account_name": "onelake",
        "account_host": "onelake.dfs.fabric.microsoft.com",
    }
    dfs = fsspec.filesystem("abfss", **STORAGE_OPTIONS)

    # Ensure parent directory exists
    notebookutils.fs.mkdirs(os.path.dirname(dest_path) or attachments_tmp_dir)

    with dfs.open(dest_path, "wb") as f:
        f.write(pdf_bytes)

    logger.info(f"Blob downloaded and saved to: {dest_path}")
    return dest_path


def process_alerts(
    template_name: str,
    storage_name: str,
    email_recipient: str,
    attachments: List[str]
) -> bool:
    """
    Process email alert: load template, replace tokens, and send with attachments.
    """
    try:
        required_params = {
            'template_name': template_name,
            'storage_name': storage_name,
            'email_recipient': email_recipient
        }
        for param, value in required_params.items():
            if not value:
                raise ValueError(f"Missing required parameter: {param}")

        replacement_tokens = {
            'storage_account': storage_name,
            'user_email': email_recipient
        }

        logger.info(f"Getting template location for: {template_name}")
        template_location = get_template_location_url(file_name=template_name)
        if not template_location:
            raise ValueError("Failed to get template location")
        logger.info(f"Template location: {template_location}")

        template_data = read_json_file(template_location)
        if not template_data:
            raise ValueError("Failed to read template file")

        processed_template = replace_tokens_in_json_object(template_data, replacement_tokens)
        if not processed_template:
            raise ValueError("Failed to process template tokens")

        # Key vault name for the email utility (existing pattern)
        secrets_scope = notebookutils.variableLibrary.get("$(/**/app_config/key_vault_name)")

        input_params = {
            "subject": processed_template.get("subject", ""),
            "body": processed_template.get("body", {}).get("content", ""),
            "to_email": processed_template.get("emailRecipient", ""),
            "from_account": processed_template.get("emailSender", ""),
            "key_vault_name": secrets_scope,
            "attachments": attachments
        }

        for param in ["subject", "body", "to_email", "from_account"]:
            if not input_params[param]:
                raise ValueError(f"Missing email parameter: {param}")

        logger.info("Sending mail ...")
        send_email(**input_params)
        logger.info("Email sent successfully")
        return True

    except Exception as e:
        logger.error(f"Alert processing error: {str(e)}", exc_info=True)
        return False


# =============================================================================
# MAIN EXECUTION
# =============================================================================
def main():
    start_time = datetime.now() if 'datetime' in dir() else None  # optional timing
    logger.info("Starting FNOL email alert processing")

    try:
        # Parse blob path from Activator subject
        match = re.search(r"/containers/([^/]+)/blobs/(.+)", blobPath)
        if not match:
            raise ValueError(f"Cannot parse blob subject: {blobPath}")

        container_name = match.group(1)
        blob_name = match.group(2)
        logger.info(f"Container: {container_name}, Blob: {blob_name}")

        # Load configuration from Variable Library
        app_config = notebookutils.variableLibrary.getLibrary("app_config")
        secrets_scope = notebookutils.variableLibrary.get("$(/**/app_config/key_vault_name)")
        email_recipient = notebookutils.variableLibrary.get("$(/**/app_config/fnol_email_recipient)")
        storage_account_name = app_config.fnolai_storage_account_name

        if not storage_account_name:
            raise ValueError("fnolai_storage_account_name is not configured")

        # Create Service Principal credential from Key Vault
        credential = get_service_principal_credential(secrets_scope)

        # Download PDF to OneLake tmp location
        pdf_path = f"{attachments_tmp_dir.rstrip('/')}/{blob_name}"
        download_blob_to_onelake(
            storage_account_name=storage_account_name,
            container_name=container_name,
            blob_name=blob_name,
            credential=credential,
            dest_path=pdf_path
        )

        # Send email with attachment
        success = process_alerts(
            template_name=template_name,
            storage_name=storage_account_name,
            email_recipient=email_recipient,
            attachments=[pdf_path]
        )

        if not success:
            logger.error("Alert processing failed")
            raise RuntimeError("Email alert processing failed")

        logger.info("FNOL email alert processing completed successfully")

    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    from datetime import datetime
    main()
