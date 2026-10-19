import re
import json
from typing import Optional

def _clean_filter_expression(self, expr: Optional[str]) -> str:
    """
    Strip a leading WHERE / AND (case-insensitive) and return a clean condition.
    Empty / None → empty string.
    """
    if not expr or not isinstance(expr, str):
        return ""
    cleaned = re.sub(r"^\s*(WHERE|AND)\s+", "", expr.strip(), count=1, flags=re.IGNORECASE)
    return cleaned.strip()


def update_watermark_query(self) -> None:
    """Update watermark query file for incremental or CDC processing."""
    # Determine watermark column based on ingest type
    watermark_column = (
        "__$start_lsn"
        if self.ingest_type == "cdc" and "__start_lsn" in self._source_data.columns
        else "dl_watermark"
        if self.source_watermark_identifier
        else None
    )

    if not watermark_column:
        self.logger.info(
            f"No valid watermark column found for {self.target_table}. "
            "Skipping watermark query file update."
        )
        return

    # Aggregate max watermark value
    try:
        agg_col = watermark_column.replace("$", "")
        source_watermark_identifier_row = (
            self._source_data.agg({agg_col: "max"}).collect()[0][0]
        )
    except Exception as e:
        self.logger.info(
            f"Error aggregating watermark column {watermark_column} in "
            f"{self.target_table}: {e}. Skipping watermark update."
        )
        return

    if source_watermark_identifier_row is None:
        self.logger.info(
            f"No records exist in {self.target_table}. "
            "Skipping watermark query file update."
        )
        return

    # Convert bytearray to SQL Server binary(10) format when needed
    if watermark_column == "__$start_lsn":
        source_watermark_identifier_value = "0x" + source_watermark_identifier_row.hex()
    else:
        source_watermark_identifier_value = source_watermark_identifier_row

    # ------------------------------------------------------------------
    # Prepare filter (fixed)
    # ------------------------------------------------------------------
    filter_condition = self._clean_filter_expression(self.filterExpression)
    filter_clause = f" AND {filter_condition}" if filter_condition else ""

    include_specific_columns = self.include_specific_columns or "*"
    source_table_schema = self.source_table_schema
    source_table = self.target_table

    # ------------------------------------------------------------------
    # Build query
    # ------------------------------------------------------------------
    if self.ingest_type == "cdc" and "__start_lsn" in self._source_data.columns:
        self.logger.info(
            f"Updating CDC watermark query file for {self.dataset_file_name} dataset"
        )
        primary_keys = ",".join(self.candidate_keys)
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
        self.logger.info(
            f"Updating watermark query file for {self.dataset_file_name} dataset"
        )

        # Handle SELECT * + watermark column correctly
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

    # indent=2 makes the query string readable when the JSON is opened
    notebookutils.fs.put(
        file_location,
        json.dumps(query_extract_json, indent=2, ensure_ascii=False),
        overwrite=True,
    )
    return self
