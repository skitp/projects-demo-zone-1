import re
import json
from typing import Optional

def _clean_filter_expression(self, expr: Optional[str]) -> str:
    """Strip leading WHERE/AND (case-insensitive)."""
    if not expr or not isinstance(expr, str):
        return ""
    return re.sub(
        r"^\s*(WHERE|AND)\s+", "", expr.strip(), count=1, flags=re.IGNORECASE
    ).strip()


def _resolve_lsn_column(self) -> Optional[str]:
    """Return the actual LSN column name present in the DataFrame, or None."""
    cols = set(self._source_data.columns)
    for candidate in ("__$start_lsn", "__start_lsn"):
        if candidate in cols:
            return candidate
    return None


def update_watermark_query(self) -> None:
    """Update watermark query file for incremental or CDC processing."""
    is_cdc = str(self.ingest_type).lower() == "cdc"
    lsn_column = self._resolve_lsn_column() if is_cdc else None

    # Decide watermark column
    if is_cdc and lsn_column:
        watermark_column = lsn_column          # keep the real name for later
        agg_column = lsn_column.replace("$", "")  # Spark-friendly name for agg
    elif self.source_watermark_identifier:
        watermark_column = "dl_watermark"
        agg_column = self.source_watermark_identifier
    else:
        self.logger.info(
            f"No valid watermark column found for {self.target_table}. "
            "Skipping watermark query file update."
        )
        return

    # Aggregate max watermark value
    try:
        source_watermark_identifier_row = (
            self._source_data.agg({agg_column: "max"}).collect()[0][0]
        )
    except Exception as e:
        self.logger.info(
            f"Error aggregating watermark column {agg_column} in "
            f"{self.target_table}: {e}. Skipping watermark update."
        )
        return

    if source_watermark_identifier_row is None:
        self.logger.info(
            f"No records exist in {self.target_table}. "
            "Skipping watermark query file update."
        )
        return

    # Convert bytearray → SQL Server binary(10) when it is an LSN
    if is_cdc:
        if isinstance(source_watermark_identifier_row, (bytes, bytearray)):
            source_watermark_identifier_value = "0x" + source_watermark_identifier_row.hex()
        else:
            # already a string / other type
            source_watermark_identifier_value = str(source_watermark_identifier_row)
    else:
        source_watermark_identifier_value = source_watermark_identifier_row

    # Filter (safe append)
    filter_condition = self._clean_filter_expression(self.filterExpression)
    filter_clause = f" AND {filter_condition}" if filter_condition else ""

    include_specific_columns = self.include_specific_columns or "*"
    source_table_schema = self.source_table_schema
    source_table = self.target_table

    # ------------------------------------------------------------------
    # Build query – CDC branch is now driven by is_cdc + successful LSN
    # ------------------------------------------------------------------
    if is_cdc:
        self.logger.info(
            f"Updating CDC watermark query file for {self.dataset_file_name} dataset"
        )
        primary_keys = ",".join(self.candidate_keys or [])
        table_name = f"[cdc].[{source_table_schema}_{source_table}_CT]"
        cdc_columns = ["__$operation", "__$start_lsn", "__$command_id", "__$seqval"]

        if include_specific_columns.strip() == "*":
            columns_list = "*"
        else:
            columns_list = f"{include_specific_columns},{','.join(cdc_columns)}"

        query = f"""WITH LatestChanges AS (
    SELECT
        {columns_list},
        ROW_NUMBER() OVER (
            PARTITION BY {primary_keys}
            ORDER BY [__$start_lsn] DESC,
                     __$command_id DESC,
                     __$seqval DESC,
                     __$operation DESC
        ) AS rn
    FROM {table_name}
    WHERE [__$operation] IN (1, 2, 3, 4)
      AND __$start_lsn >= {source_watermark_identifier_value}{filter_clause}
)
SELECT
    {columns_list}
FROM LatestChanges
WHERE rn = 1"""

        source_watermark_id_out = "__$start_lsn"

    else:
        # Regular watermark path – only reached when source_watermark_identifier is set
        self.logger.info(
            f"Updating watermark query file for {self.dataset_file_name} dataset"
        )
        if include_specific_columns.strip() == "*":
            select_list = f"*, {self.source_watermark_identifier} AS dl_watermark"
        else:
            select_list = (
                f"{include_specific_columns}, "
                f"{self.source_watermark_identifier} AS dl_watermark"
            )

        query = f"""SELECT
    {select_list}
FROM {source_table_schema}.{source_table}
WHERE {self.source_watermark_identifier} > CAST('{source_watermark_identifier_value}' AS datetime2){filter_clause}"""

        source_watermark_id_out = self.source_watermark_identifier

    # ------------------------------------------------------------------
    # Write pretty-printed JSON
    # ------------------------------------------------------------------
    query_extract_json = {
        "query": query.strip(),
        "sourceWatermarkIdentifier": source_watermark_id_out,
        "watermarkValue": str(source_watermark_identifier_value),
    }

    if not self._metadata_lakehouse:
        self.metadata_lakehouse(self.metadata_lakehouse_name, self._metadata_workspace_id)

    datasets_folder = f"datasets_{self.product_name}" if self.product_name else "datasets"
    file_location = (
        f"{self._metadata_lakehouse.lakehouse_path}/Files/"
        f"{datasets_folder}/{self.dataset_config_folder_name}/watermark/"
        f"{self.dataset_file_name}.json"
    )

    notebookutils.fs.put(
        file_location,
        json.dumps(query_extract_json, indent=2, ensure_ascii=False),
        overwrite=True,
    )
    return self
