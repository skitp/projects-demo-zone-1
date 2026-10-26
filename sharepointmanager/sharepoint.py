"""
SharePointManager
-----------------
Production-ready SharePoint file operations for the gdap_spark project.

Features:
- Download file(s) / folders from SharePoint → Lakehouse Files
- Upload DataFrame / Delta table / Excel → SharePoint folder
- Excel (.xlsx) support on both download and upload
- Retry with exponential backoff
- Resumable large-file uploads
- Folder listing
- Uses existing GDAPLogging from gap_logging
- Service Principal + Azure Key Vault
"""

import os
import time
import logging
import requests
from typing import Optional, List, Dict, Union, Any
from urllib.parse import quote
from functools import wraps
from contextlib import contextmanager

from pyspark.sql import DataFrame, SparkSession
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

# Existing project logging
from gap_logging import GDAPLogging

try:
    from mssparkutils.fs import getMountPath
except ImportError:
    def getMountPath(path: str) -> str:
        return path


def retry(max_attempts: int = 3, base_delay: float = 1.0, max_delay: float = 30.0):
    """Exponential backoff for transient HTTP errors."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            delay = base_delay
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    status = getattr(getattr(e, "response", None), "status_code", None)
                    if status and status < 500 and status != 429:
                        raise
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    logger = kwargs.get("logger") or getattr(args[0], "logger", logging.getLogger(__name__))
                    logger.warning(
                        f"Retry {attempt}/{max_attempts} after error: {e}. Sleeping {delay:.1f}s"
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)
        return wrapper
    return decorator


class SharePointManager:
    """
    Production SharePoint manager using Microsoft Graph + Service Principal.

    Expected Key Vault secrets:
        - sp-client-id
        - sp-client-secret
        - sp-tenant-id
        - sharepoint-site-id   (optional)
    """

    def __init__(
        self,
        key_vault_url: str,
        site_id: Optional[str] = None,
        spark: Optional[SparkSession] = None,
        max_retries: int = 3,
        logger_name: str = "SharePointManager"
    ):
        self.key_vault_url = key_vault_url.rstrip("/")
        self.site_id = site_id
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.max_retries = max_retries

        # Use existing GDAPLogging
        self.logger = GDAPLogging.get_logger(logger_name)

        self._load_credentials()
        self._token = None
        self._token_expires = 0

    # ------------------------------------------------------------------
    # Credential & Token handling
    # ------------------------------------------------------------------
    def _load_credentials(self):
        try:
            self.credential = ClientSecretCredential(
                tenant_id=self._get_secret("sp-tenant-id"),
                client_id=self._get_secret("sp-client-id"),
                client_secret=self._get_secret("sp-client-secret")
            )
            self.logger.info("Service Principal credentials loaded from Key Vault")
        except Exception as e:
            self.logger.error(f"Failed to load credentials from Key Vault: {e}")
            raise

    def _get_secret(self, secret_name: str) -> str:
        try:
            client = SecretClient(vault_url=self.key_vault_url, credential=self.credential)
            return client.get_secret(secret_name).value
        except Exception:
            try:
                from notebookutils import mssparkutils
                return mssparkutils.credentials.getSecret(self.key_vault_url, secret_name)
            except Exception as e:
                self.logger.error(f"Could not retrieve secret '{secret_name}': {e}")
                raise

    def _get_access_token(self) -> str:
        if self._token and time.time() < self._token_expires - 60:
            return self._token
        token = self.credential.get_token("https://graph.microsoft.com/.default")
        self._token = token.token
        self._token_expires = token.expires_on
        self.logger.debug("Obtained new Microsoft Graph access token")
        return self._token

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------
    @retry()
    def list_folder(
        self,
        sharepoint_folder_path: str,
        site_id: Optional[str] = None,
        recursive: bool = False
    ) -> List[Dict[str, Any]]:
        site_id = site_id or self.site_id
        if not site_id:
            raise ValueError("site_id is required")

        token = self._get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        folder_url = self._resolve_item_url(site_id, sharepoint_folder_path)
        children_url = f"{folder_url}/children"

        items = []
        while children_url:
            resp = requests.get(children_url, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("value", []):
                entry = {
                    "name": item["name"],
                    "path": f"{sharepoint_folder_path.rstrip('/')}/{item['name']}",
                    "size": item.get("size"),
                    "is_folder": "folder" in item,
                    "last_modified": item.get("lastModifiedDateTime"),
                    "id": item.get("id"),
                    "mime_type": item.get("file", {}).get("mimeType")
                }
                items.append(entry)
                if recursive and entry["is_folder"]:
                    items.extend(self.list_folder(entry["path"], site_id, recursive=True))
            children_url = data.get("@odata.nextLink")

        self.logger.info(f"Listed {len(items)} items in {sharepoint_folder_path}")
        return items

    # ------------------------------------------------------------------
    # Download operations
    # ------------------------------------------------------------------
    @retry()
    def download_file(
        self,
        sharepoint_path: str,
        lakehouse_target_path: str,
        site_id: Optional[str] = None
    ) -> str:
        site_id = site_id or self.site_id
        if not site_id:
            raise ValueError("site_id must be provided")

        token = self._get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        item_url = self._resolve_item_url(site_id, sharepoint_path)
        download_url = f"{item_url}/content"

        self.logger.info(f"Downloading file from SharePoint: {sharepoint_path}")

        response = requests.get(download_url, headers=headers, stream=True, timeout=120)
        response.raise_for_status()

        full_target = os.path.join(getMountPath("/lakehouse/default"), "Files", lakehouse_target_path)
        os.makedirs(os.path.dirname(full_target), exist_ok=True)

        with open(full_target, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        self.logger.info(f"File saved to Lakehouse: {full_target}")
        return full_target

    def download_folder(
        self,
        sharepoint_folder_path: str,
        lakehouse_target_folder: str,
        site_id: Optional[str] = None,
        recursive: bool = True
    ) -> List[str]:
        items = self.list_folder(sharepoint_folder_path, site_id, recursive=False)
        downloaded = []

        for item in items:
            if item["is_folder"] and recursive:
                sub_lh = f"{lakehouse_target_folder.rstrip('/')}/{item['name']}"
                downloaded.extend(
                    self.download_folder(item["path"], sub_lh, site_id, recursive)
                )
            elif not item["is_folder"]:
                lh_path = f"{lakehouse_target_folder.rstrip('/')}/{item['name']}"
                full = self.download_file(item["path"], lh_path, site_id)
                downloaded.append(full)

        return downloaded

    # ------------------------------------------------------------------
    # Upload operations (Excel, CSV, Parquet)
    # ------------------------------------------------------------------
    def upload_dataframe(
        self,
        df: DataFrame,
        sharepoint_folder_path: str,
        file_name: str,
        format: str = "csv",
        site_id: Optional[str] = None,
        sheet_name: str = "Sheet1",
        **write_options
    ) -> str:
        """
        Upload DataFrame as CSV, Parquet, or Excel (.xlsx).
        """
        site_id = site_id or self.site_id
        temp_dir = f"/tmp/sp_upload_{int(time.time())}"
        os.makedirs(temp_dir, exist_ok=True)

        local_path = None
        try:
            if format.lower() in ("csv", "parquet"):
                writer = df.write.mode("overwrite")
                for k, v in write_options.items():
                    writer = writer.option(k, v)

                if format.lower() == "csv":
                    writer.option("header", "true").csv(temp_dir)
                    part = next(f for f in os.listdir(temp_dir) if f.startswith("part-"))
                    local_path = os.path.join(temp_dir, part)
                else:  # parquet
                    writer.parquet(temp_dir)
                    part = next(f for f in os.listdir(temp_dir) if f.endswith(".parquet"))
                    local_path = os.path.join(temp_dir, part)

            elif format.lower() in ("excel", "xlsx"):
                # Excel via pandas (collect may be expensive for very large frames)
                pdf = df.toPandas()
                local_path = os.path.join(temp_dir, file_name if file_name.endswith(".xlsx") else f"{file_name}.xlsx")
                pdf.to_excel(local_path, index=False, sheet_name=sheet_name, engine="openpyxl")
                self.logger.info(f"Wrote Excel file with {len(pdf)} rows to {local_path}")
            else:
                raise ValueError("Supported formats: csv, parquet, excel/xlsx")

            return self._upload_file(local_path, sharepoint_folder_path, file_name, site_id)

        finally:
            if local_path and os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass

    def upload_delta_table(
        self,
        delta_table_path: str,
        sharepoint_folder_path: str,
        file_name: str,
        format: str = "parquet",
        site_id: Optional[str] = None,
        sheet_name: str = "Sheet1"
    ) -> str:
        df = self.spark.read.format("delta").load(delta_table_path)
        return self.upload_dataframe(
            df, sharepoint_folder_path, file_name, format, site_id, sheet_name=sheet_name
        )

    # ------------------------------------------------------------------
    # Ingest / Export style helpers
    # ------------------------------------------------------------------
    def ingest_from_sharepoint(
        self,
        sharepoint_path: str,
        lakehouse_target_path: str,
        site_id: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        full_path = self.download_file(sharepoint_path, lakehouse_target_path, site_id)
        size = os.path.getsize(full_path)
        meta = {
            "source": "sharepoint",
            "sharepoint_path": sharepoint_path,
            "lakehouse_path": full_path,
            "file_size_bytes": size,
            "ingested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        self.logger.info(f"Ingest completed: {meta}")
        return meta

    def export_to_sharepoint(
        self,
        source: Union[DataFrame, str],
        sharepoint_folder_path: str,
        file_name: str,
        format: str = "csv",
        site_id: Optional[str] = None,
        sheet_name: str = "Sheet1",
        **kwargs
    ) -> Dict[str, Any]:
        if isinstance(source, str):
            result_path = self.upload_delta_table(
                source, sharepoint_folder_path, file_name, format, site_id, sheet_name
            )
        else:
            result_path = self.upload_dataframe(
                source, sharepoint_folder_path, file_name, format, site_id, sheet_name, **kwargs
            )

        meta = {
            "destination": "sharepoint",
            "sharepoint_path": result_path,
            "file_name": file_name,
            "format": format,
            "exported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        self.logger.info(f"Export completed: {meta}")
        return meta

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _resolve_item_url(self, site_id: str, path: str) -> str:
        encoded = quote(path.lstrip("/"))
        return f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{encoded}"

    def _upload_file(self, local_file_path: str, sharepoint_folder_path: str, file_name: str, site_id: str) -> str:
        file_size = os.path.getsize(local_file_path)
        if file_size < 4 * 1024 * 1024:
            return self._simple_upload(local_file_path, sharepoint_folder_path, file_name, site_id)
        else:
            return self._resumable_upload(local_file_path, sharepoint_folder_path, file_name, site_id)

    @retry()
    def _simple_upload(self, local_path: str, folder_path: str, file_name: str, site_id: str) -> str:
        token = self._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream"
        }
        target = f"{folder_path.rstrip('/')}/{file_name}"
        upload_url = self._resolve_item_url(site_id, target) + ":/content"

        self.logger.info(f"Simple upload: {local_path} → {target}")
        with open(local_path, "rb") as f:
            resp = requests.put(upload_url, headers=headers, data=f, timeout=120)
            resp.raise_for_status()
        return target

    def _resumable_upload(self, local_path: str, folder_path: str, file_name: str, site_id: str) -> str:
        token = self._get_access_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        target = f"{folder_path.rstrip('/')}/{file_name}"
        create_session_url = self._resolve_item_url(site_id, target) + ":/createUploadSession"

        session_resp = requests.post(create_session_url, headers=headers, json={}, timeout=60)
        session_resp.raise_for_status()
        upload_url = session_resp.json()["uploadUrl"]

        file_size = os.path.getsize(local_path)
        chunk_size = 5 * 1024 * 1024
        with open(local_path, "rb") as f:
            offset = 0
            while offset < file_size:
                chunk = f.read(chunk_size)
                end = offset + len(chunk) - 1
                content_range = f"bytes {offset}-{end}/{file_size}"
                chunk_headers = {
                    "Content-Length": str(len(chunk)),
                    "Content-Range": content_range
                }
                resp = requests.put(upload_url, headers=chunk_headers, data=chunk, timeout=120)
                resp.raise_for_status()
                offset += len(chunk)
                self.logger.debug(f"Uploaded chunk {offset}/{file_size}")

        self.logger.info(f"Resumable upload completed: {target}")
        return target
