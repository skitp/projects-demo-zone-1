def create_dataset_files_cdc() -> None:
    """
    Generate per-dataset JSON configuration files for incremental load based on dictionary and metadata tables:
        - curated_dataset_list
        - curated_dataset_column_list

    Returns:
        None: Saves JSON files to the output directory.
    """
    dataset_subfolder = "iis_cdc"

    # Load metadata tables
    # 1. Source columns
    dl_table_path = f"{lhs_path_root}/Tables/catalog/source_dataset_column_list"
    query = f"""
        SELECT dataset_name, column_name, column_data_type FROM delta.`{dl_table_path}`
    """
    logger.info(f"Getting metadata from {dl_table_path} table...")
    source_column_df = spark.sql(query)

    # 2. Matched SQL DB with ADO requested (cdc) tables
    dl_table_path_cdc = f"{lhs_path_root}/Tables/catalog/cdc_dataset_list_crqry"
    dl_table_path_sdr = f"{lhs_path_root}/Tables/catalog/source_dataset_request"
    query = f"""
        SELECT 
            sdr.database_name, sdr.schema_name, sdr.table_name
        FROM delta.`{dl_table_path_cdc}` as cdc
        INNER JOIN
            delta.`{dl_table_path_sdr}` as sdr
        ON sdr.database_name = cdc.database_name
        AND sdr.schema_name = cdc.dataset_schema
        AND sdr.table_name = cdc.dataset_name
        WHERE sdr.status = 'initiated'
    """
    logger.info(f"Getting metadata from {dl_table_path_cdc} table...")
    cdc_dataset_df = spark.sql(query)

    # 3. Get primary keys from backfilled and SQL DB metadata tables
    dl_table_path_pk = f"{lhs_path_root}/Tables/catalog/source_dataset_pk_list"
    dl_table_path_sdl = f"{lhs_path_root}/Tables/catalog/source_dataset_list"
    query = f"""
        SELECT 
            sdl.database_name, sdl.dataset_schema, sdl.dataset_name, COALESCE(sdl.primary_key_list, pk.primary_key_list) AS primary_key_list
        FROM delta.`{dl_table_path_sdl}` as sdl
        LEFT JOIN delta.`{dl_table_path_pk}` as pk
            ON sdl.database_name = pk.database_name
            AND sdl.dataset_schema = pk.dataset_schema
            AND sdl.dataset_name = pk.dataset_name;
        """
    logger.info(f"Getting metadata from {dl_table_path_pk} table...")
    pk_dataset_df = spark.sql(query)


    # Get unique datasets
    datasets = source_column_df.select("dataset_name").distinct().collect()

    #for dataset_name in datasets:
    for row in datasets:
        dataset_name = row[0]
        # Filter curated columns for this dataset
        ds_curated = source_column_df.filter(f"dataset_name = '{dataset_name}'")

        if ds_curated.count() == 0:
            print(f"Skipping {dataset_name}: No columns to load.")
            continue

        columns = ds_curated.select("column_name").rdd.map(lambda row: row[0]).collect()
        data_types_rows = ds_curated.select("column_name", "column_data_type").collect()
        data_types = {row["column_name"]: row["column_data_type"] for row in data_types_rows}

        # Set defaults
        partition_keys = []
        additional_selects = []
        is_dynamic_query = True

        # CDC ingest type:
        ingest_type = 'cdc'
        watermark_id = None

        # Build includeSpecificColumns list
        select_list = []
        for col_name in columns:
            col_lower = col_name.lower()
            dt = data_types.get(col_name, '').lower()
            if dt == 'geography':
                select_list.append(f"CAST({col_lower} AS VARCHAR(255)) AS {col_lower}")
            elif dt == 'char':
                select_list.append(f"TRIM({col_lower}) AS {col_lower}")
            else:
                select_list.append(col_lower)

        primary_key_list = []

        # Get primaryKeyList from curated_dataset_list (assuming it's a comma-separated string)
        pk_row = pk_dataset_df.filter(f"dataset_name = '{dataset_name}'").select("primary_key_list", "database_name", "dataset_schema").first()

        primary_key_list = json.loads(pk_row[0]) if pk_row and pk_row[0] else []
        primary_key_list = [c.lower().strip() for c in primary_key_list]

        # Get database name and schema name from curated_dataset_list
        database_name = pk_row[1]
        dataset_schema = pk_row[2]

        # Determine targetLoadType
        target_load_type = "merge" if primary_key_list and ingest_type != "full" else "overwrite"

        # Determine filter expression
        base_filter = filter_map.get(dataset_name)

        filter_expression = None
        if base_filter:
            # Normilize: make sure it starts with AND if needed
            cleaned = base_filter.strip()
            if watermark_id:
                # For watermark / incremental loads → must start with AND
                if not cleaned.upper().startswith("AND "):
                    filter_expression = "AND " + cleaned
                else:
                    filter_expression = cleaned
            else:
                # For full loads → we keep it as provided
                if cleaned.upper().startswith("AND "):
                    filter_expression = cleaned[4:].strip()  # remove leading AND
                else:
                    filter_expression = cleaned

        #  Build sourceSystemProperties
        source_props = {
            "sourceSystemName": "IIS",
            "includeSpecificColumns": select_list,
            "ingestType": ingest_type,
            "isDynamicQuery": is_dynamic_query
        }

        if filter_expression:
            source_props["filterExpression"] = filter_expression

        # Build the dataset config dictionary
        config = {
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
                "removeSourceFiles": True
            },
            "curatedProperties": {
                "lakehouseName": lakehouse_name,
                "schemaName": database_name,
                "primaryKeyList": primary_key_list,
                "duplicateCheckEnabled": False,
                "targetFileFormat": "delta",
                "targetLoadType": target_load_type
            }
        }

        # Save dataset to JSON file
        dataset_json_path = f"{datasets_path}/{dataset_subfolder}/{dataset_name}.json"
        with dfs.open(dataset_json_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)

        print(f"Generated config for {dataset_name}: {dataset_json_path}")
      def create_dataset_files_incremental() -> None:
    """
    Generate per-dataset JSON configuration files for incremental load based on dictionary and metadata tables:
        - curated_dataset_list
        - curated_dataset_column_list

    Returns:
        None: Saves JSON files to the output directory.
    """
    dataset_subfolder = "iis_base"

    # Load metadata tables
    # 1. Source columns
    dl_table_path = f"{lhs_path_root}/Tables/catalog/source_dataset_column_list"
    query = f"""
        SELECT dataset_name, column_name, column_data_type FROM delta.`{dl_table_path}`
    """
    logger.info(f"Getting metadata from {dl_table_path} table...")
    source_column_df = spark.sql(query)

    # 2. Matched SQL DB with ADO requested (new) tables
    dl_table_path_sdl = f"{lhs_path_root}/Tables/catalog/source_dataset_list"
    dl_table_path_sdr = f"{lhs_path_root}/Tables/catalog/source_dataset_request"
    query = f"""
        SELECT 
            sdr.database_name, sdr.schema_name, sdr.table_name
        FROM delta.`{dl_table_path_sdr}` as sdr
        INNER JOIN
            delta.`{dl_table_path_sdl}` as sdl
        ON sdr.database_name = sdl.database_name
        AND sdr.schema_name = sdl.dataset_schema
        AND sdr.table_name = sdl.dataset_name
        WHERE sdr.status = 'initiated'
    """
    logger.info(f"Getting metadata from {dl_table_path_sdr} table...")
    incremental_dataset_df = spark.sql(query)

    # 3. Get primary keys from backfilled and SQL DB metadata tables
    dl_table_path_pk = f"{lhs_path_root}/Tables/catalog/source_dataset_pk_list"
    query = f"""
        SELECT 
            sdl.database_name, sdl.dataset_schema, sdl.dataset_name, COALESCE(sdl.primary_key_list, pk.primary_key_list) AS primary_key_list
        FROM delta.`{dl_table_path_sdl}` as sdl
        LEFT JOIN delta.`{dl_table_path_pk}` as pk
            ON sdl.database_name = pk.database_name
            AND sdl.dataset_schema = pk.dataset_schema
            AND sdl.dataset_name = pk.dataset_name;
        """
    logger.info(f"Getting metadata from {dl_table_path_pk} table...")
    pk_dataset_df = spark.sql(query)


    # Get unique datasets
    datasets = source_column_df.select("dataset_name").distinct().collect()

    #for dataset_name in datasets:
    for row in datasets:
        dataset_name = row[0]
        # Filter curated columns for this dataset
        ds_curated = source_column_df.filter(f"dataset_name = '{dataset_name}'")

        if ds_curated.count() == 0:
            print(f"Skipping {dataset_name}: No columns to load.")
            continue

        columns = ds_curated.select("column_name").rdd.map(lambda row: row[0]).collect()
        data_types_rows = ds_curated.select("column_name", "column_data_type").collect()
        data_types = {row["column_name"]: row["column_data_type"] for row in data_types_rows}

        # Set defaults
        partition_keys = []
        additional_selects = []
        is_dynamic_query = True

        # Default ingest type:
        ingest_type = None
        watermark_id = None
        if ingest_type:
            watermark_id = None

        else:
            lower_columns = [
                col.lower().strip('[]') for col in columns
            ]
            has_updateon = 'updateon' in lower_columns
            has_enteredon = 'enteredon' in lower_columns

            if has_updateon or has_enteredon:
                ingest_type = "watermark"
                if has_updateon and has_enteredon:
                    watermark_id = "COALESCE(updateon, enteredon)"
                elif has_updateon:
                    watermark_id = "updateon"
                elif has_enteredon:
                    watermark_id = "enteredon"
            else: 
                ingest_type = "full"
                watermark_id = None

        # Build includeSpecificColumns list
        select_list = []
        for col_name in columns:
            col_lower = col_name.lower()
            dt = data_types.get(col_name, '').lower()
            if dt == 'geography':
                select_list.append(f"CAST({col_lower} AS VARCHAR(255)) AS {col_lower}")
            elif dt == 'char':
                select_list.append(f"TRIM({col_lower}) AS {col_lower}")
            else:
                select_list.append(col_lower)

        primary_key_list = []

        # Get primaryKeyList from curated_dataset_list (assuming it's a comma-separated string)
        pk_row = pk_dataset_df.filter(f"dataset_name = '{dataset_name}'").select("primary_key_list", "database_name", "dataset_schema").first()

        primary_key_list = json.loads(pk_row[0]) if pk_row and pk_row[0] else []
        primary_key_list = [c.lower().strip() for c in primary_key_list]

        # Get database name and schema name from curated_dataset_list
        database_name = pk_row[1]
        dataset_schema = pk_row[2]

        # Determine targetLoadType
        target_load_type = "merge" if primary_key_list and ingest_type != "full" else "overwrite"

        # Determine filter expression
        base_filter = filter_map.get(dataset_name)

        filter_expression = None
        if base_filter:
            # Normilize: make sure it starts with AND if needed
            cleaned = base_filter.strip()
            if watermark_id:
                # For watermark / incremental loads → must start with AND
                if not cleaned.upper().startswith("AND "):
                    filter_expression = "AND " + cleaned
                else:
                    filter_expression = cleaned
            else:
                # For full loads → we keep it as provided
                if cleaned.upper().startswith("AND "):
                    filter_expression = cleaned[4:].strip()  # remove leading AND
                else:
                    filter_expression = cleaned

        #  Build sourceSystemProperties
        source_props = {
            "sourceSystemName": "IIS",
            "includeSpecificColumns": select_list,
            "ingestType": ingest_type,
            "isDynamicQuery": is_dynamic_query
        }

        if watermark_id:
            source_props["sourceWatermarkIdentifier"] = watermark_id

        if filter_expression:
            source_props["filterExpression"] = filter_expression

        # Build the dataset config dictionary
        config = {
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
                "removeSourceFiles": True
            },
            "curatedProperties": {
                "lakehouseName": lakehouse_name,
                "schemaName": database_name,
                "primaryKeyList": primary_key_list,
                "duplicateCheckEnabled": False,
                "targetFileFormat": "delta",
                "targetLoadType": target_load_type
            }
        }

        # Save dataset to JSON file
        dataset_json_path = f"{datasets_path}/{dataset_subfolder}/{dataset_name}.json"
        with dfs.open(dataset_json_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)

        print(f"Generated config for {dataset_name}: {dataset_json_path}")
