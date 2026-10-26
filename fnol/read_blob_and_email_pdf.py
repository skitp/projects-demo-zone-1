import re
import tempfile
import os
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AccessToken, TokenCredential
import notebookutils

# ---------- 1. Parameters (from Activator) ----------
# blobPath is already set by the parameter cell, e.g.:
# /blobServices/default/containers/mycontainer/blobs/chatbot-pdfs/output/report-123.pdf

# ---------- 2. Parse the subject ----------
match = re.search(r"/containers/([^/]+)/blobs/(.+)", blobPath)
if not match:
    raise ValueError(f"Cannot parse blob subject: {blobPath}")

container_name = match.group(1)
blob_name = match.group(2)
print(f"Container: {container_name}")
print(f"Blob: {blob_name}")

# ---------- 3. Custom credential that uses notebookutils token ----------
class NotebookUtilsCredential(TokenCredential):
    def get_token(self, *scopes, **kwargs):
        token = notebookutils.credentials.getToken("storage")
        # Token is valid for ~1 hour; set a safe expiry
        return AccessToken(token, int(__import__("time").time()) + 3500)

# ---------- 4. Download the blob ----------
storage_account = "<your-storage-account-name>"  # hard-code or pass as another parameter
account_url = f"https://{storage_account}.blob.core.windows.net"

credential = NotebookUtilsCredential()
blob_service = BlobServiceClient(account_url=account_url, credential=credential)
blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)

# Option A: download to memory (good for small/medium PDFs)
pdf_bytes = blob_client.download_blob().readall()

# Option B: download to a local temp file (preferred for email attachment)
with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
    download_stream = blob_client.download_blob()
    tmp.write(download_stream.readall())
    local_pdf_path = tmp.name

print(f"Downloaded to: {local_pdf_path}")

# ---------- 5. Use the file with your existing email logic ----------
# Example (adapt to your existing send-email function):
# send_email(
#     subject="Your generated PDF",
#     body="Please find the PDF attached.",
#     attachment_path=local_pdf_path,
#     attachment_name=os.path.basename(blob_name)
# )

# Clean up if you want
# os.remove(local_pdf_path)

# Alterantive: mount
# Mount the container (uses Entra token of the notebook runner by default)
mount_point = "/mnt/chatbot_pdfs"
notebookutils.fs.mount(
    f"abfss://{container_name}@{storage_account}.dfs.core.windows.net",
    mount_point
)

local_path = notebookutils.fs.getMountPath(mount_point)
full_local_path = f"{local_path}/{blob_name}"

# Now you can open it like a normal file
with open(full_local_path, "rb") as f:
    pdf_bytes = f.read()
