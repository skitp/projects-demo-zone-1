def update_watermark_query(self) -> None:
        """Update watermark query file for incremental or CDC processing."""
        # Determine watermark column based on ingest type
        watermark_column = (
            "__$start_lsn" if self.ingest_type == "cdc" and "__start_lsn" in self._source_data.columns
            else "dl_watermark" if self.source_watermark_identifier
            else None
        )
        
        if not watermark_column:
            self.logger.info(f"No valid watermark column found for {self.target_table}. Skipping watermark query file update.")
            return
        
        # Aggregate max watermark value
        try:
            source_watermark_identifier_row = self._source_data.agg({watermark_column.replace('$',''): "max"}).collect()[0][0]
        except Exception as e:
            self.logger.info(f"Error aggregating watermark column {watermark_column} in {self.target_table}: {str(e)}. Skipping watermark update.")
            return

        if source_watermark_identifier_row is None:
            self.logger.info(f"No records exist in {self.target_table}. Skipping watermark query file update.")
            return
        
        # convert a bytearray to SQL Server binary(10) format:
        if watermark_column == "__$start_lsn":
            source_watermark_identifier_value = '0x' + source_watermark_identifier_row.hex() 
        else:
            source_watermark_identifier_value = source_watermark_identifier_row

        # Build query based on ingest type
        include_specific_columns = self.include_specific_columns
        filter_expression = self.filterExpression
        source_table_schema = self.source_table_schema
        source_table = self.target_table

        if self.ingest_type == "cdc" and "__start_lsn" in self._source_data.columns:
            self.logger.info(f"Updating CDC watermark query file for {self.dataset_file_name} dataset")
            primary_keys = ",".join(self.candidate_keys)
            table_name = f"[cdc].[{source_table_schema}_{source_table}_CT]"
            cdc_columns = ["__$operation", "__$start_lsn", "__$command_id"] 
            columnsList = f"{include_specific_columns},{','.join(cdc_columns)}" if include_specific_columns != "*" else "*"
            query = f"""
                WITH LatestChanges AS (
                    SELECT 
                        {columnsList},
                        ROW_NUMBER() OVER (
                            PARTITION BY {primary_keys}
                            ORDER BY [__$start_lsn] DESC, __$command_id DESC,__$seqval DESC,__$operation DESC
                        ) AS rn
                    FROM {table_name}
                    WHERE [__$operation] IN (1, 2, 3, 4) AND __$start_lsn >= {source_watermark_identifier_value}{filter_expression}
                )
                SELECT 
                    {columnsList}
                FROM LatestChanges
                WHERE rn = 1
            """
        else:
            self.logger.info(f"Updating watermark query file for {self.dataset_file_name} dataset")
            query = f"""
                SELECT {include_specific_columns}, {self.source_watermark_identifier} as dl_watermark  
                FROM {source_table_schema}.{source_table}
                WHERE {self.source_watermark_identifier} > CAST('{source_watermark_identifier_value}' AS datetime2){filter_expression}
            """

        # Write watermark query to file
        query_extract_json = {
            "query": query.strip(),
            "sourceWatermarkIdentifier": self.source_watermark_identifier if watermark_column == "dl_watermark" else watermark_column,
            "watermarkValue": str(source_watermark_identifier_value)
        }
        if not self._metadata_lakehouse:
            self.metadata_lakehouse(self.metadata_lakehouse_name, self._metadata_workspace_id)

        datasets_folder = f"datasets_{self.product_name}" if self.product_name else "datasets"
        file_location = (
            f"{self._metadata_lakehouse.lakehouse_path}/Files/"
            f"{datasets_folder}/{self.dataset_config_folder_name}/watermark/"
            f"{self.dataset_file_name}.json"
)
        notebookutils.fs.put(file_location, json.dumps(query_extract_json), overwrite=True)
        return self
