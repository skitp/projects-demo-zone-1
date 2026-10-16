import fsspec
import json
import logging
import re
from typing import List, Dict, Optional

# ---------------------------------------------------------------------------
# Configuration (override via notebook / locals() when running in Fabric)
# ---------------------------------------------------------------------------
source_name = locals().get("source_name") or "sqlcrqry"
init_watermark_value = locals().get("init_watermark_value") or "1983-01-01T00:00:00Z"
top_rows = locals().get("top_rows")
datasets_folder = locals().get("datasets_folder") or "datasets"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def validate_path(path: str) -> bool:
    """Return True if path is a non-empty string."""
    if not path or not isinstance(path, str):
        logger.error("Invalid path provided")
        return False
    return True


def list_folder_contents(path: str, result_list: Optional[List[Dict]] = None) -> List[Dict]:
    """
    Recursively list files under path, skipping any 'watermark' folder.
    """
    if not validate_path(path):
        return []
    if result_list is None:
        result_list = []

    try:
        items = notebookutils.fs.ls(path)
        for item in items:
            if item.isDir:
                folder_name = item.name.rstrip("/")
                if folder_name != "watermark":
                    logger.info(f"Processing folder: {folder_name}")
                    list_folder_contents(item.path, result_list)
            else:
                parent_folder = path.split("/")[-1] if "/" in path else ""
                if parent_folder != "watermark":
                    result_list.append({
                        "folderName": parent_folder,
                        "fileName": item.name
                    })
                    logger.debug(
                        f"Added file: folderName={parent_folder}, fileName={item.name}"
                    )
    except Exception as e:
        logger.error(f"Error accessing {path}: {e}")

    return result_list


def _clean_filter_expression(expr: str) -> str:
    """
    Strip a leading WHERE or AND (case-insensitive) and surrounding whitespace.
    Leaves the rest of the expression unchanged (preserves case of literals).
    """
    if not expr:
        return ""
    return re.sub(r"^\s*(WHERE|AND)\s+", "", expr, count=1, flags=re.IGNORECASE).strip()


def generate_watermark(
    source_config_folder_name: str,
    source_config_file_name: str,
    storage_options: Dict,
    init_watermark_value: str,
    top_rows: Optional[int] = None
) -> None:
    """
    Generate the watermark JSON for a single dataset configuration.
    """
    try:
        input_path = f"{source_config_folder_name}{source_config_file_name}"
        watermark_path = f"{source_config_folder_name}watermark/{source_config_file_name}"

        if not validate_path(input_path):
            raise ValueError("Invalid input path")

        onelake_fs = fsspec.filesystem("abfss", **storage_options)

        with onelake_fs.open(input_path, "r") as f:
            dataset = json.load(f)

        source_system_properties = dataset.get("sourceSystemProperties", {})
        curated_properties = dataset.get("curatedProperties", {})

        if dataset.get("datasetTypeName") not in ("database", "file"):
            logger.warning(f"Unsupported dataset type: {dataset.get('datasetTypeName')}")
            return

        # ------------------------------------------------------------------
        # Columns
        # ------------------------------------------------------------------
        include_cols = source_system_properties.get("includeSpecificColumns") or ["*"]
        columns_list = ",".join(include_cols)

        where_clause = ""
        watermark_value = init_watermark_value
        source_watermark_identifier = ""
        query = ""

        ingest_type = source_system_properties.get("ingestType")

        # ------------------------------------------------------------------
        # Watermark ingest
        # ------------------------------------------------------------------
        if ingest_type == "watermark":
            source_watermark_identifier = source_system_properties.get(
                "sourceWatermarkIdentifier", ""
            )
            if not source_watermark_identifier:
                raise ValueError(
                    "sourceWatermarkIdentifier is required for watermark ingest type"
                )

            # Add watermark column (handle SELECT * correctly)
            if columns_list.strip() == "*":
                columns_list = f"*, {source_watermark_identifier} AS dl_watermark"
            else:
                columns_list += f",{source_watermark_identifier} AS dl_watermark"

            if onelake_fs.exists(watermark_path):
                with onelake_fs.open(watermark_path, "r", encoding="utf-8-sig") as f:
                    existing = json.load(f)
                    if existing.get("sourceWatermarkIdentifier") == source_watermark_identifier:
                        watermark_value = existing.get("watermarkValue", watermark_value)

            where_clause = (
                f"WHERE {source_watermark_identifier} >= CAST('{watermark_value}' AS datetime2)"
            )

        # ------------------------------------------------------------------
        # CDC ingest
        # ------------------------------------------------------------------
        elif ingest_type == "cdc":
            primary_key_list = curated_properties.get("primaryKeyList") or []
            if not primary_key_list:
                raise ValueError("Primary key list is required for CDC ingest type")

            cdc_columns = ["__$operation", "__$start_lsn", "__$command_id", "__$seqval"]
            if columns_list.strip() != "*":
                columns_list = f"{columns_list},{','.join(cdc_columns)}"

            source_watermark_identifier = "__$start_lsn"
            watermark_value = "0x0"

            if onelake_fs.exists(watermark_path):
                with onelake_fs.open(watermark_path, "r", encoding="utf-8-sig") as f:
                    existing = json.load(f)
                    if existing.get("sourceWatermarkIdentifier") == source_watermark_identifier:
                        watermark_value = existing.get("watermarkValue", watermark_value)

            partition_keys = ",".join(primary_key_list)
            table_name = (
                f"[cdc].[{dataset.get('datasetSchema', 'dbo')}_{dataset['datasetName']}_CT]"
            )

            query = f"""WITH LatestChanges AS (
    SELECT {columns_list},
           ROW_NUMBER() OVER (
               PARTITION BY {partition_keys}
               ORDER BY [__$start_lsn] DESC, __$command_id DESC, __$seqval DESC
           ) AS rn
    FROM {table_name}
    WHERE [__$operation] IN (1, 2, 3, 4)
      AND __$start_lsn >= {watermark_value}
)
SELECT {columns_list}
FROM LatestChanges
WHERE rn = 1"""

        # ------------------------------------------------------------------
        # Filter expression (bug fix)
        # ------------------------------------------------------------------
        raw_filter = source_system_properties.get("filterExpression", "").strip()
        filter_expression = _clean_filter_expression(raw_filter)

        # Apply filter only when isDynamicQuery is true (original behaviour)
        # or always when a filter is present – change the condition if desired.
        apply_filter = (
            source_system_properties.get("isDynamicQuery") and filter_expression
        )

        if apply_filter:
            if ingest_type == "cdc":
                # Attach to the outer SELECT of the CTE query
                query = f"{query} AND {filter_expression}"
            else:
                if where_clause:
                    where_clause = f"{where_clause} AND {filter_expression}"
                else:
                    where_clause = f"WHERE {filter_expression}"

        # ------------------------------------------------------------------
        # Build final query for non-CDC types
        # ------------------------------------------------------------------
        if ingest_type != "cdc":
            schema = dataset.get("datasetSchema", "dbo")
            table = dataset["datasetName"]
            query = (
                f"SELECT {columns_list} "
                f"FROM {schema}.{table} "
                f"{where_clause}"
            ).strip()

        # Optional TOP
        if top_rows is not None:
            query = query.replace("SELECT", f"SELECT TOP({top_rows})", 1)

        # ------------------------------------------------------------------
        # Persist watermark file
        # ------------------------------------------------------------------
        json_watermark = {
            "query": query,
            "sourceWatermarkIdentifier": source_watermark_identifier,
            "watermarkValue": watermark_value,
        }

        with onelake_fs.open(watermark_path, "w") as f:
            json.dump(json_watermark, f, indent=4)

        logger.info(f"Watermark file generated at: {watermark_path}")

    except Exception as e:
        logger.error(f"Failed to generate watermark for {source_config_file_name}: {e}")
        raise


def main() -> None:
    """Orchestrate watermark generation for all datasets under the source folder."""
    try:
        storage_options = {
            "account_name": "onelake",
            "account_host": "onelake.dfs.fabric.microsoft.com",
        }

        lakehouse = notebookutils.lakehouse.get("den_lhw_pdi_001_metadata")
        workspace_id = lakehouse["workspaceId"]
        lakehouse_id = lakehouse["id"]

        suffix = f"_{source_name}" if source_name else ""
        datasets_path = (
            f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{lakehouse_id}/Files/{datasets_folder}{suffix}"
        )
        logger.info(f"Processing datasets from: {datasets_path}")

        contents = list_folder_contents(datasets_path)
        if not contents:
            logger.warning("No dataset files found")
            return

        for item in contents:
            source_config_folder_name = f"{datasets_path}/{item['folderName']}/"
            source_config_file_name = item["fileName"]
            logger.info(f"Generating watermark for: {source_config_file_name}")
            generate_watermark(
                source_config_folder_name,
                source_config_file_name,
                storage_options,
                init_watermark_value,
                top_rows,
            )

        logger.info("Watermark generation completed successfully")

    except Exception as e:
        logger.error(f"Watermark generation failed: {e}")
        raise


if __name__ == "__main__":
    main()
