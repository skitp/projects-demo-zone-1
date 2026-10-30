import json
import re
import os
from typing import Dict, Optional
import logging
import fsspec
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AccessToken, TokenCredential
import notebookutils

from spark_engine.common.email_util import send_email 
from spark_engine.common.lakehouse import LakehouseManager



# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Parameters (from Activator)
# blobPath is already set by the parameter cell, e.g.:
blobPath = "/blobServices/default/containers/fnol-claims-pdfs/blobs/GT-5801d372.pdf"



# Parse the subject
match = re.search(r"/containers/([^/]+)/blobs/(.+)", blobPath)
if not match:
    raise ValueError(f"Cannot parse blob subject: {blobPath}")

container_name = match.group(1)
blob_name = match.group(2)
print(f"Container: {container_name}")
print(f"Blob: {blob_name}")



# Configurations and Constants
app_config = notebookutils.variableLibrary.getLibrary("app_config")
secretsScope = notebookutils.variableLibrary.get("$(/**/app_config/key_vault_name)")
email_recipient = notebookutils.variableLibrary.get("$(/**/app_config/fnol_email_recipient)")
storage_account_name = app_config.fnolai_storage_account_name
template_name = "fnolai_web_entry.json"
attachments_tmp_dir = "tmp"



# Custom credential that uses notebookutils token
class NotebookUtilsCredential(TokenCredential):
    def get_token(self, *scopes, **kwargs):
        token = notebookutils.credentials.getToken("storage")
        # Token is valid for ~1 hour; set a safe expiry
        return AccessToken(token, int(__import__("time").time()) + 3400)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def get_template_location_url(
    lakehouse_name: str = "den_lhw_pdi_001_metadata",
    notification_type: str = "emails",
    file_name: str = ""
) -> Optional[str]:
    """
    Get the URL for the template location in the lakehouse.
    
    Args:
        lakehouse_name: Name of the lakehouse
        notification_type: Type of notification (e.g., emails)
        file_name: Name of the template file
    
    Returns:
        String URL path or None if error occurs
    """
    try:
        lakehouse_manager = LakehouseManager(lakehouse_name=lakehouse_name)
        template_path = f"{lakehouse_manager.lakehouse_path}/Files/templates/{notification_type}"
        return f"{template_path}/{file_name}"
    except Exception as e:
        logger.error(f"Failed to get template location URL: {str(e)}")
        return None

def replace_tokens_in_json_object(json_object: Dict, param_dict: Dict) -> Optional[Dict]:
    """
    Replace tokens in JSON object with parameter values.
    
    Args:
        json_object: JSON object to process
        param_dict: Dictionary of token-value pairs
    
    Returns:
        Processed JSON dictionary or None if error occurs
    """
    try:
        value = json.dumps(json_object)
        for k, v in param_dict.items():
            # Ensure value is string and handle None values
            v = str(v) if v is not None else ""
            value = value.replace('{' + k + '}', v)
        return json.loads(value)
    except (json.JSONDecodeError, TypeError) as e:
        logger.error(f"Error replacing tokens in JSON: {str(e)}")
        return None

def read_json_file(file_location: str) -> Optional[Dict]:
    """
    Read and parse JSON file from the specified location.
    
    Args:
        file_location: Path to the JSON file
    
    Returns:
        Parsed JSON dictionary or None if error occurs
    """
    try:    
        jsonDf = spark.read.text(file_location, wholetext=True)
        content = jsonDf.first()["value"]
        return json.loads(content)
    except (json.JSONDecodeError, Exception) as e:
        logger.error(f"Error reading JSON file {file_location}: {str(e)}")
        return None


# Set filesystem options
STORAGE_OPTIONS = {
    "account_name": "onelake",
    "account_host": "onelake.dfs.fabric.microsoft.com",
}
dfs = fsspec.filesystem("abfss", **STORAGE_OPTIONS)


# Create directory
notebookutils.fs.mkdirs(attachments_tmp_dir)

# Get filename
# file_name = os.path.basename(blob_client.blob_name)

# ABFSS destination
pdf_path = f"{attachments_tmp_dir.rstrip('/')}/{blob_name}"

# Download PDF from Blob Storage
pdf_bytes = blob_client.download_blob().readall()

# Write binary data directly to ABFSS
with dfs.open(pdf_path, "wb") as f:
    f.write(pdf_bytes)

print(f"Downloaded to: {pdf_path}")


def process_alerts(
    template_name: str,
    storage_name: str,
    email_recipient: str,
    attachments: list[dict]
) -> bool:
    """
    Main function to process alerts and send email notifications.
    
    Returns:
        True if email sent successfully, False otherwise
    """
    try:
        # Validate required parameters
        required_params = {
            'template_name': template_name,
            'storage_name': storage_name,
            'email_recipient': email_recipient
        }
        
        for param, value in required_params.items():
            if not value:
                logger.error(f"Missing required parameter: {param}")
                raise ValueError(f"Missing required parameter: {param}")
        
        # Create replacement tokens dictionary
        replacement_tokens = {
            'storage_account': storage_name,
            'user_email': email_recipient
        }

        # Get template location
        logger.info(f"Getting template location for: {template_name}")
        template_location = get_template_location_url(file_name=template_name)
        if not template_location:
            raise ValueError("Failed to get template location")
        logger.info(f"Template location: {template_location}")
        # Read and process template
        template_data = read_json_file(template_location)
        if not template_data:
            raise ValueError("Failed to read template file")

        # Replace tokens
        processed_template = replace_tokens_in_json_object(template_data, replacement_tokens)
        if not processed_template:
            raise ValueError("Failed to process template tokens")

        # Prepare email parameters
        input_params = {
            "subject": processed_template.get("subject", ""),
            "body": processed_template.get("body", {}).get("content", ""),
            "to_email": processed_template.get("emailRecipient", ""),
            "from_account": processed_template.get("emailSender", ""),
            "key_vault_name": secretsScope,
            "attachments": attachments
        }

        # Validate email parameters
        for param in ["subject", "body", "to_email", "from_account"]:
            if not input_params[param]:
                logger.error(f"Missing email parameter: {param}")
                raise ValueError(f"Missing email parameter: {param}")

        # Send email
        logger.info("Sending mail ...")
        send_email(**input_params)
        return True

    except Exception as e:
        logger.error(f"Alert processing error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in alert processing: {str(e)}", exc_info=True)
        return False


if __name__ == "__main__":
    # process alerts
    success = process_alerts(
        template_name=template_name,
        storage_name=storage_account_name,
        email_recipient=email_recipient,
        attachments=[pdf_path]
    )
if not success:
    logger.error("Alert processing failed")
    exit(1)
