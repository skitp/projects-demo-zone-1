 # ------------------------------------------------------------------
    # 4. Extended properties and ownership metadata (shared)
    # ------------------------------------------------------------------
    dl_table_path_sdr = f"{lhs_path_root}/Tables/catalog/source_dataset_request"
    dl_table_path_exp = f"{lhs_path_root}/Tables/catalog/table_ext_properties_crqry"
    query = f"""
        SELECT
            sdr.database_name,
            sdr.schema_name,
            sdr.table_name,
            sdr.domain_owner,
            exp.extended_property_value	
        FROM delta.`{dl_table_path_sdr}` AS sdr
        LEFT JOIN delta.`{dl_table_path_exp}` AS exp
            ON  sdr.database_name = exp.database_name
            AND sdr.schema_name = exp.dataset_schema
            AND sdr.table_name=exp.dataset_name
        WHERE exp.extended_property_name = 'ms_description'
    """
    logger.info(f"Loading primary-key metadata from {dl_table_path_pk}")
    exp_dataset_df: DataFrame = spark.sql(query)

"deltaTableProperties": {
        "description": exp.extended_property_value,
        "owner": "DnA Platform Services",
        "tblproperties": {
            "governance.data_domain": sdr.domain_owner,
            "governance.sensitivity": "Private",
            "refresh_cadence": "Mon-Fri"
        }
    }




from typing import Literal, Optional, List, Dict, Any
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
import json


def create_dataset_files(mode: Literal["cdc", "incremental"] = "incremental") -> None:
    """
    Generate per-dataset JSON configuration files for IIS source datasets.

    Supports two modes:
      - "cdc"         → CDC ingest (subfolder: iis_cdc)
      - "incremental" → watermark or full load (subfolder: iis_base)

    Metadata sources:
      - source_dataset_column_list
      - source_dataset_pk_list / source_dataset_list
      - source_dataset_request (+ cdc_dataset_list_crqry for CDC mode)

    Only datasets with status = 'initiated' in source_dataset_request are processed.

    Returns:
        None. Writes one JSON file per dataset under {datasets_path}/{subfolder}/.
    """
    if mode not in ("cdc", "incremental"):
        raise ValueError(f"mode must be 'cdc' or 'incremental', got '{mode}'")

    dataset_subfolder = "iis_cdc" if mode == "cdc" else "iis_base"
    logger.info(f"Starting dataset config generation in mode='{mode}' → subfolder '{dataset_subfolder}'")

    # ------------------------------------------------------------------
    # 1. Common metadata: source columns
    # ------------------------------------------------------------------
    dl_table_path = f"{lhs_path_root}/Tables/catalog/source_dataset_column_list"
    query = f"""
        SELECT dataset_name, column_name, column_data_type
        FROM delta.`{dl_table_path}`
    """
    logger.info(f"Loading column metadata from {dl_table_path}")
    source_column_df: DataFrame = spark.sql(query)

    # ------------------------------------------------------------------
    # 2. Mode-specific list of datasets that are ready to process
    # ------------------------------------------------------------------
    if mode == "cdc":
        dl_table_path_cdc = f"{lhs_path_root}/Tables/catalog/cdc_dataset_list_crqry"
        dl_table_path_sdr = f"{lhs_path_root}/Tables/catalog/source_dataset_request"
        query = f"""
            SELECT
                sdr.database_name,
                sdr.schema_name,
                sdr.table_name
            FROM delta.`{dl_table_path_cdc}` AS cdc
            INNER JOIN delta.`{dl_table_path_sdr}` AS sdr
                ON  sdr.database_name = cdc.database_name
                AND sdr.schema_name   = cdc.dataset_schema
                AND sdr.table_name    = cdc.dataset_name
            WHERE sdr.status = 'initiated'
        """
        logger.info(f"Loading CDC-initiated datasets from {dl_table_path_cdc}")
    else:  # incremental
        dl_table_path_sdl = f"{lhs_path_root}/Tables/catalog/source_dataset_list"
        dl_table_path_sdr = f"{lhs_path_root}/Tables/catalog/source_dataset_request"
        query = f"""
            SELECT
                sdr.database_name,
                sdr.schema_name,
                sdr.table_name
            FROM delta.`{dl_table_path_sdr}` AS sdr
            INNER JOIN delta.`{dl_table_path_sdl}` AS sdl
                ON  sdr.database_name = sdl.database_name
                AND sdr.schema_name   = sdl.dataset_schema
                AND sdr.table_name    = sdl.dataset_name
            WHERE sdr.status = 'initiated'
        """
        logger.info(f"Loading incremental-initiated datasets from {dl_table_path_sdr}")

    request_df: DataFrame = spark.sql(query)
    datasets_to_process: List[Row] = (
        request_df
        .select("database_name", "schema_name", "table_name")
        .distinct()
        .collect()
    )

    if not datasets_to_process:
        logger.warning(f"No initiated datasets found for mode='{mode}'. Nothing to generate.")
        return

    # ------------------------------------------------------------------
    # 3. Primary-key metadata (shared)
    # ------------------------------------------------------------------
    dl_table_path_pk  = f"{lhs_path_root}/Tables/catalog/source_dataset_pk_list"
    dl_table_path_sdl = f"{lhs_path_root}/Tables/catalog/source_dataset_list"
    query = f"""
        SELECT
            sdl.database_name,
            sdl.dataset_schema,
            sdl.dataset_name,
            COALESCE(sdl.primary_key_list, pk.primary_key_list) AS primary_key_list
        FROM delta.`{dl_table_path_sdl}` AS sdl
        LEFT JOIN delta.`{dl_table_path_pk}` AS pk
            ON  sdl.database_name  = pk.database_name
            AND sdl.dataset_schema = pk.dataset_schema
            AND sdl.dataset_name   = pk.dataset_name
    """
    logger.info(f"Loading primary-key metadata from {dl_table_path_pk}")
    pk_dataset_df: DataFrame = spark.sql(query)

    # ------------------------------------------------------------------
    # 4. Process each requested dataset
    # ------------------------------------------------------------------
    generated_count = 0
    skipped_count   = 0

    for row in datasets_to_process:
        database_name  = row["database_name"]
        dataset_schema = row["schema_name"]
        dataset_name   = row["table_name"]

        # ---- columns ----
        ds_curated = source_column_df.filter(F.col("dataset_name") == dataset_name)
        if ds_curated.isEmpty():
            logger.warning(f"Skipping {database_name}.{dataset_schema}.{dataset_name}: no columns found in source_dataset_column_list")
            skipped_count += 1
            continue

        col_rows = ds_curated.select("column_name", "column_data_type").collect()
        columns: List[str] = [r["column_name"] for r in col_rows]
        data_types: Dict[str, str] = {r["column_name"]: r["column_data_type"] for r in col_rows}

        # ---- ingest type + watermark ----
        if mode == "cdc":
            ingest_type: Optional[str] = "cdc"
            watermark_id: Optional[str] = None
        else:
            lower_columns = [c.lower().strip("[]") for c in columns]
            has_updateon  = "updateon"  in lower_columns
            has_enteredon = "enteredon" in lower_columns

            if has_updateon or has_enteredon:
                ingest_type = "watermark"
                if has_updateon and has_enteredon:
                    watermark_id = "COALESCE(updateon, enteredon)"
                elif has_updateon:
                    watermark_id = "updateon"
                else:
                    watermark_id = "enteredon"
            else:
                ingest_type = "full"
                watermark_id = None

        # ---- includeSpecificColumns (with type-aware transforms) ----
        select_list: List[str] = []
        for col_name in columns:
            col_lower = col_name.lower()
            dt = data_types.get(col_name, "").lower()
            if dt == "geography":
                select_list.append(f"CAST({col_lower} AS VARCHAR(255)) AS {col_lower}")
            elif dt == "char":
                select_list.append(f"TRIM({col_lower}) AS {col_lower}")
            else:
                select_list.append(col_lower)

        # ---- primary keys (strict match on db + schema + name) ----
        pk_row = (
            pk_dataset_df
            .filter(
                (F.col("database_name")  == database_name) &
                (F.col("dataset_schema") == dataset_schema) &
                (F.col("dataset_name")   == dataset_name)
            )
            .select("primary_key_list")
            .first()
        )

        primary_key_list: List[str] = []
        if pk_row and pk_row["primary_key_list"]:
            try:
                primary_key_list = json.loads(pk_row["primary_key_list"])
                primary_key_list = [c.lower().strip() for c in primary_key_list]
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(
                    f"{database_name}.{dataset_schema}.{dataset_name}: "
                    f"could not parse primary_key_list → treating as empty. Error: {e}"
                )
                primary_key_list = []

        # ---- target load type ----
        target_load_type = "merge" if primary_key_list and ingest_type != "full" else "overwrite"

        # ---- filter expression (normalised) ----
        filter_expression: Optional[str] = None
        base_filter = filter_map.get(dataset_name)
        if base_filter:
            cleaned = base_filter.strip()
            if watermark_id:
                # watermark / cdc path → force leading AND
                filter_expression = cleaned if cleaned.upper().startswith("AND ") else f"AND {cleaned}"
            else:
                # full load → strip leading AND if present
                filter_expression = cleaned[4:].strip() if cleaned.upper().startswith("AND ") else cleaned

        # ---- sourceSystemProperties ----
        source_props: Dict[str, Any] = {
            "sourceSystemName": "IIS",
            "includeSpecificColumns": select_list,
            "ingestType": ingest_type,
            "isDynamicQuery": True,
        }
        if watermark_id:
            source_props["sourceWatermarkIdentifier"] = watermark_id
        if filter_expression:
            source_props["filterExpression"] = filter_expression

        # ---- final config ----
        config: Dict[str, Any] = {
            "datasetName": dataset_name,
            "enable": True,
            "datasetTypeName": "database",
            "databaseName": database_name,
            "datasetSchema": dataset_schema,
            "skipProductLoad": False,
            "sourceSystemProperties": source_props,
            "rawProperties": {
                "lakehouseName": lakehouse_name,
                "fileType": "parquet",
                "directoryName": f"iis_{database_name}".upper(),
                "removeSourceFiles": True,
            },
            "curatedProperties": {
                "lakehouseName": lakehouse_name,
                "schemaName": database_name,
                "primaryKeyList": primary_key_list,
                "duplicateCheckEnabled": False,
                "targetFileFormat": "delta",
                "targetLoadType": target_load_type,
            },
        }

        # ---- write JSON ----
        dataset_json_path = f"{datasets_path}/{dataset_subfolder}/{dataset_name}.json"
        try:
            with dfs.open(dataset_json_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=4)
            logger.info(f"Generated config → {dataset_json_path}")
            generated_count += 1
        except Exception as e:
            logger.error(f"Failed to write {dataset_json_path}: {e}")
            skipped_count += 1

    logger.info(
        f"Finished mode='{mode}'. "
        f"Generated={generated_count}, Skipped={skipped_count}, "
        f"Total requested={len(datasets_to_process)}"
    )

create_dataset_files("cdc")          # → iis_cdc/
create_dataset_files("incremental")  # → iis_base/
create_dataset_files()               # default = incremental

Area,Before,After
Structure,Two nearly identical functions,Single function with mode parameter
Dataset selection,Loaded request tables but never used them – processed all columns,Only processes datasets that are actually status = 'initiated'
PK lookup,Filtered only by dataset_name (fragile),Strict 3-part key match (db + schema + name)
Error handling,Crashed on missing PK / bad JSON,Graceful fallback + warning
Logging,Mixed print / logger,Consistent structured logging + final summary
Dead code,"Unused partition_keys, additional_selects, dead if ingest_type",Removed
Type safety,None,Full type hints + Literal
Column collection,Unnecessary RDD map,Clean .collect()
Filter expression,"Same logic, but now clearly documented",Explicit comment on AND normalisation rules

