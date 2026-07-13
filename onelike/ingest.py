from pyspark.sql.functions import col, row_number, when, lit, max as max_
from pyspark.sql.window import Window
import json
import re
from datetime import datetime, timezone
from pyspark.sql.dataframe import DataFrame
from typing import List, Optional

from spark_engine.ingestion.metadata import Metadata
from spark_engine.common.lakehouse import LakehouseManager, SchemaManager
from spark_engine.ingestion.strategy import LoadTypes
from spark_engine.common.file_handler import FileHandler
from spark_engine.ingestion.metrics import IngestionMetrics, GroupMetrics
from spark_engine.data_quality.quarantine import QuarantineVersion, QuarantineLog
from spark_engine.data_quality.data_quality import Expectations

import notebookutils
from pyspark.sql.session import SparkSession
from spark_engine.common.gdap_logging import GDAPLogging
from spark_engine.sparkconf import spark


class Ingest:
    """
    The Ingest class which takes the metadata config and processes a single table to the lakehouse.
    Production-ready version using only Liquid Clustering (no partitioning).
    Clustering keys = dl_lastmodifiedutc + primaryKeyList (candidate_keys).
    Improved structured logging.
    """

    schema_manager = SchemaManager()
    METRICS: IngestionMetrics # fix rist load output?

    def __init__(self, ingest_config: str) -> None:
        config = (
            ingest_config
            if isinstance(ingest_config, dict)
            else json.loads(ingest_config)
        )
        self.batch_time = datetime.now(timezone.utc).isoformat()
        self.load_type = config["curatedProperties"]["targetLoadType"]
        self.source_format = config["rawProperties"]["fileType"].lower()
        self.source_options = config["curatedProperties"].get("fileOptions")
        self.source_system = config["sourceSystemProperties"]["sourceSystemName"]
        self.target_table = config["datasetName"]
        self.source_table_schema = config.get("datasetSchema")
        self.target_schema = config["curatedProperties"]["schemaName"]
        self.target_lakehouse_name = config["curatedProperties"]["lakehouseName"]
        self.source_lakehouse_name = config["rawProperties"]["lakehouseName"]
        self.column_list = config["curatedProperties"].get("columnList")
        self.candidate_keys = config["curatedProperties"].get("primaryKeyList")
        self.partition_keys = config["curatedProperties"].get("partitionKeyList")
        self.custom_file_options = config["curatedProperties"].get("customFileOptions")
        self.duplicate_check_enabled = config["curatedProperties"].get(
            "duplicateCheckEnabled"
        )
        self.duplicate_check_quarantine_type = config["curatedProperties"].get(
            "duplicateCheckQuarantineType"
        )

        self.dataset_type_name = config["datasetTypeName"]
        self.ingest_type = config["sourceSystemProperties"]["ingestType"]

        self.is_dynamic_query = config["sourceSystemProperties"].get("isDynamicQuery")
        self.include_specific_columns = config["sourceSystemProperties"].get("includeSpecificColumns")
        self.source_watermark_identifier = config["sourceSystemProperties"].get("sourceWatermarkIdentifier")
        self.metadata_lakehouse_name = "den_lhw_pdi_001_metadata"

        self.raise_errors = config["curatedProperties"].get("raiseErrors", True)
        self.data_quality_enabled = config["curatedProperties"].get("dataQualityEnabled", False)
        self.expectations = config["curatedProperties"].get("expectations") # remove in next
        self.quarantine_strategy = config["curatedProperties"].get("quarantineStrategy")
        self.is_cdc = config["sourceSystemProperties"].get("isCdc", False)  # Flag for CDC processing   

        # Lakehouses can be set either using the methods explicitly or by the name provided in the ingest config
        # Lakehouse instances
        self._target_lakehouse = None
        self._source_lakehouse = None
        self._metadata_lakehouse = None

        self._source_workspace_id = None
        self._target_workspace_id = None
        self._metadata_workspace_id = None

        # Logging setup - use target_table as context in logger name
        logger_name = f"ingest.{self.target_table}"
        self.logger = GDAPLogging(
            logger_name=logger_name,
            logger_level=10,          # DEBUG
            root_log_level=10,        # DEBUG for root too (can be adjusted)
            logging_format_spec='%(levelname)s [%(asctime)s] [%(name)s] %(funcName)s:%(lineno)d - %(message)s'
        ).get_gdap_logger()

        self.logger.info(f"Initialized Ingest for table '{self.target_schema}.{self.target_table}' "
                         f"with load type '{self.load_type}' at {self.batch_time}")
        
        if "partitionKeyList" in config["curatedProperties"]:
            self.logger.warning("partitionKeyList is deprecated - using Liquid Clustering only")
        
        self.cluster_by = self.candidate_keys if self.candidate_keys else []


        self._validate_config()
        self._set_load_strategy(self.load_type)

        # Prepare include_specific_columns
        self.include_specific_columns = (
            ",".join(config["sourceSystemProperties"]["includeSpecificColumns"])
            if "includeSpecificColumns" in config["sourceSystemProperties"]
            else "*"
        )

        # check if filterExpression is present
        self.filterExpression = " " + config["sourceSystemProperties"].get("filterExpression", "")

        # self.METRICS = IngestionMetrics()
        self.ALLMETRICS = GroupMetrics()

    def _validate_config(self):
        required = {
            "targetLoadType": self.load_type,
            "schemaName": self.target_schema,
            "lakehouseName": self.target_lakehouse_name,
        }
        for k, v in required.items():
            if not v:
                raise ValueError(f"Missing required field: {k}")

        if self.load_type == "merge" and not self.candidate_keys:
            raise ValueError("Merge load requires primaryKeyList (used for clustering)")

        self.logger.debug("Configuration validated")

    def target_lakehouse(self, lakehouse_name: str, workspace_id: Optional[str] = None):
        """
        Set the target lakehouse (with optional workspace for cross-workspace support).
        """
        self._target_workspace_id = workspace_id
        self._target_lakehouse = LakehouseManager(lakehouse_name, workspace_id)
        self.logger.debug(f"Target lakehouse set: {lakehouse_name} (workspace_id={workspace_id})")
        return self

    def source_lakehouse(self, lakehouse_name: str, workspace_id: Optional[str] = None):
        """
        Set the source lakehouse (with optional workspace for cross-workspace support).
        """
        self._source_workspace_id = workspace_id
        self._source_lakehouse = LakehouseManager(lakehouse_name, workspace_id)
        self.logger.debug(f"Source lakehouse set: {lakehouse_name} (workspace_id={workspace_id})")
        return self
    
    def metadata_lakehouse(self, lakehouse_name: str, workspace_id: Optional[str] = None):
        """
        Set the metadata lakehouse (with optional workspace for cross-workspace support).
        """
        self._metadata_workspace_id = workspace_id
        self._metadata_lakehouse = LakehouseManager(lakehouse_name, workspace_id)
        self.logger.debug(f"Metadata lakehouse set: {lakehouse_name} (workspace_id={workspace_id})")
        return self

    def _set_load_strategy(self, load_type: str):
        load_type = load_type.upper()
        try:
            self._load_type = LoadTypes.get_load_type(load_type)
        except AttributeError as e:
            raise ValueError(f"Load strategy: {load_type} is not supported.") from e
        self.logger.debug(f"Load strategy set: {load_type}")
        return self
        

    def source_data(self, source_file: str):
        """
        The path to the source file to be ingested. This is provided here outside of the config.
        """
        if not self._source_lakehouse:
            self.source_lakehouse(self.source_lakehouse_name, self._source_workspace_id)

        regex_lakehouse_path = "^abfss:\/\/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}@onelake.dfs.fabric.microsoft.com\/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}"
        if re.search(regex_lakehouse_path, source_file):
            self.source_file = source_file
        else:
            self.source_file = (
                f"{self._source_lakehouse.lakehouse_path}/Files/{source_file}"
            )

        self._source_data = self._read_source_data(
            self.source_file,
            self.source_format,
            self.source_options,
            self.custom_file_options,
        )
        return self

    def _read_source_data(
        self,
        source_file: str,
        source_format: str,
        source_options: str,
        custom_file_options: str,
    ) -> DataFrame:
        file_handler = FileHandler(source_format, source_options, custom_file_options)
        return file_handler.read_file(source_file)

    def _set_candidate_keys(self, candidate_keys: List[str]):
        if not candidate_keys:
            return None

        candidate_key_list = [item.lower() for item in candidate_keys]
        if not all(item in self._source_data.columns for item in candidate_key_list):
            raise ValueError(
                "There are columns in the Candidate Key that are not in the datafile!"
            )
        return candidate_key_list

    def _set_partition_keys(self, partition_keys: List[str]) -> List[str]:
        if not partition_keys:
            return ["dl_partitionkey", "dl_iscurrent"]
        return partition_keys
    
    def _log_quarantine(self, quarantine_metrics: list[dict]):
        quarantine_log = QuarantineLog()
        for q in quarantine_metrics:
            quarantine_log.add_log_data(
                source_system = self.source_system,
                dataset_name = self.target_table,
                elt_id = self.elt_id,
                run_id = self.run_id,
                total_rows = q["inserts"],
                quarantine_type = q["quarantine_type"],
                log_datetime = str(datetime.now(timezone.utc).isoformat()),
                primary_key_list = ",".join(self.candidate_keys if self.candidate_keys else [])
            )
        quarantine_log.write_log()

    # --- CDC Processing ---
    def _process_cdc_data(self, source_data: DataFrame) -> DataFrame:
        """
        Process CDC data to handle inserts (__$operation = 2), deletes (__$operation = 1), and updates (__$operation = 4).
        Skipped if is_cdc is False, as the Parquet file already contains net changes.
        """
        if not self.is_cdc:
            return source_data

        window_spec = Window.partitionBy(*self.candidate_keys).orderBy("__$start_lsn", "__$seqval")
        cdc_df = source_data.withColumn("row_num", row_number().over(window_spec))
        latest_changes = cdc_df.groupBy(*self.candidate_keys).agg(max_("row_num").alias("max_row_num"))
        latest_cdc_df = cdc_df.join(
            latest_changes,
            (cdc_df[self.candidate_keys[0]] == latest_changes[self.candidate_keys[0]]) &
            (cdc_df["row_num"] == latest_changes["max_row_num"]),
            "inner"
        ).select(cdc_df["*"])

        processed_df = latest_cdc_df.withColumn(
            "dl_is_deleted",
            when(col("__$operation") == 1, lit(1)).otherwise(lit(0))
        ).withColumn(
            "dl_iscurrent",
            when(col("__$operation") == 1, lit(0)).otherwise(lit(1))
        ).withColumn(
            "dl_lastmodifiedutc",
            lit(self.batch_time)
        ).select(self.column_list + ["dl_is_deleted", "dl_iscurrent", "dl_lastmodifiedutc"])

        return processed_df

    def start_ingest(self, elt_id: str = "0", run_id: str = "0") -> None:
        """
        Public entry point for ingestion.
        Chains configuration and executes the load.
        The target lakehouse will be set using the lakehouse name from the config
        if it was not explicitly set beforehand.
        """
        self.elt_id = elt_id
        self.run_id = run_id

        if not self._target_lakehouse:
            self.target_lakehouse(self.target_lakehouse_name, self._target_workspace_id)

        self.logger.info(f"start_ingest called | elt_id={elt_id} | run_id={run_id}")
        
        quarantine_table = None

        self.METRICS = IngestionMetrics()
        self.ALLMETRICS = GroupMetrics()
        self.METRICS.eltId = self.elt_id
        self.METRICS.runId = self.run_id
        self.METRICS.startTime = self.batch_time
        self.METRICS.lakehouse = self.target_lakehouse_name
        self.METRICS.deltaSchema = self.target_schema
        self.METRICS.deltaObject = self.target_table

        try:
            if self._source_data is None:
                raise ValueError("No source data loaded. Call source_data() first.")

            self.METRICS.sourceDataCount = self._source_data.count()
            self.logger.info(f"Source data ready - {self.METRICS.sourceDataCount:,} rows")

            if self.column_list:
                self._source_data = self._source_data.selectExpr(*self.column_list)
            """
            self._source_data = (
                self._source_data.selectExpr(self.column_list)
                if self.column_list
                else self._source_data
            )
            """
            metadata = Metadata(
                {
                    "meta_column_prefix": "dl_",
                    "batch_time": self.batch_time,
                    "elt_id": self.elt_id,
                    "run_id": self.run_id,
                },
                sorted(self._source_data.columns),
            )
            self._source_data = metadata.add_ingest_metadata_columns(self._source_data)

            # Reorder columns -> clustering keys first
            cluster_keys = ["dl_lastmodifiedutc"] + (self.candidate_keys or [])
            all_cols = self._source_data.columns
            priority = [c for c in cluster_keys if c in all_cols]
            rest = [c for c in all_cols if c not in priority]
            self._source_data = self._source_data.select(*(priority + rest))
            self.logger.debug(f"Clustering columns moved to front: {priority}")

            # Clean names
            self.target_schema = self.schema_manager.clean_schema_name(self.target_schema)
            self.target_table  = self.schema_manager.clean_table_name(self.target_table)


            table_path = f"{self._target_lakehouse.lakehouse_path}/Tables/{self.target_schema}/{self.target_table}"
            target_exists = self._target_lakehouse.check_if_table_exists(self.target_table, self.target_schema)

            self.logger.debug(f"Target table: | table_path={table_path} | target_exists={target_exists}")

            # Create table if not exists - with CLUSTER BY
            if not target_exists:
                self._create_clustered_table(table_path)
                target_exists = True
                
            quarantine_table = (
                QuarantineVersion(None).configure(table_path).set_current_version()
            )

            # # Configure load strategy (no partition_keys)
            self._load_type.configure_load(
                table_path=table_path,
                candidate_keys=self.candidate_keys,
                batch_time=self.batch_time,
            )

            # Deduplication check
            if self.duplicate_check_enabled:
                self._source_data = self._load_type.deduplicate(
                    self._source_data, self.duplicate_check_quarantine_type
                )

                if hasattr(self._load_type, 'quarantine_metrics') and self._load_type.quarantine_metrics:
                    self.ALLMETRICS.quarantine.append(self._load_type.quarantine_metrics)

            
            # === DQ Observability Metrics ===
            self.ALLMETRICS.dq_enabled = self.data_quality_enabled
            self.ALLMETRICS.dq_executed = False
            self.ALLMETRICS.dq_status = "skipped" if not self.data_quality_enabled else "pending"
            self.ALLMETRICS.expectations = None
            
            expectation_success = True

            self.logger.debug(f"Data quality expectations set to: '{self.data_quality_enabled}')")
            if self.data_quality_enabled:
                dq = (
                    Expectations(
                        self.target_lakehouse_name,
                        self.target_schema,
                        self.target_table,
                        self._source_data,
                        workspace_id=self._target_workspace_id,
                    )
                    .set_expectations(
                        self.target_table,
                        self.target_schema,
                        self.target_lakehouse_name
                    )
                )

                if dq.expectations:
                    self.logger.info("Running DQ expectations")
                    dq.perform_expectations(self.candidate_keys)
                    dq.quarantine(self.quarantine_strategy)
                    self._source_data = dq.expectation_df.drop(
                        *[c for c in dq.expectation_df.columns if c.startswith("dq_")]
                    )
                    if dq.quarantine_metrics:
                        self.ALLMETRICS.quarantine.append(dq.quarantine_metrics)
                    self.ALLMETRICS.expectations = dq.expectation_metrics
                    expectation_success = dq.expectation_metrics.get("success", True)

                    # Update observability status
                    self.ALLMETRICS.dq_executed = True
                    self.ALLMETRICS.dq_status = "passed" if expectation_success else "failed"
                else:
                    self.logger.info("No expectations defined for this dataset")
                    self.ALLMETRICS.dq_status = "no_expectations_defined"
                    self.ALLMETRICS.dq_executed = False
            else:
                self.logger.info("Data quality expectations skipped (dataQualityEnabled=false in config)")
                self.ALLMETRICS.dq_status = "skipped"
                self.ALLMETRICS.dq_executed = False
                
            if expectation_success:
                self.logger.debug(f"Ready to capture table columns for: '{self.target_table}'")
                
                # Capture final columns for OneLake security roles
                try:
                    final_columns = self._source_data.columns if self._source_data is not None else []
                    self.ALLMETRICS.table_columns = {
                        self.target_table: final_columns
                    }
                    self.logger.debug(f"Captured {len(final_columns)} columns for table '{self.target_table}'")
                except Exception as e:
                    self.logger.warning(f"Could not capture table columns: {e}")
                    self.ALLMETRICS.table_columns = {}

                self.logger.info(f"Load type: - {self.load_type} and target_exists - {target_exists}")
                # Execute load
                if self.load_type == "merge" and not target_exists:
                    self._load_type.first_time_load(self._source_data)
                else:
                    self._load_type.ingest(self._source_data)

                self.METRICS.success = True
                inserts, updates, deletes, output_bytes = self._load_type.get_table_metrics(table_path)
                self.METRICS.recordInserts = inserts
                self.METRICS.recordUpdates = updates
                self.METRICS.recordDeletes = deletes
                self.METRICS.outputBytes = output_bytes

            else:
                self.logger.warning("DQ expectations failed - skipping load (expectation_success=False)")
                self.METRICS.success = False

            self.METRICS.endTime = datetime.now(timezone.utc).isoformat()

            self.ALLMETRICS.ingestion = self.METRICS.to_dict()

            if self.ALLMETRICS.quarantine:
                self._log_quarantine(self.ALLMETRICS.quarantine)
                
        except Exception as e:
            self.METRICS.error = str(e)
            self.logger.error(f"Ingestion failed: {str(e)}", exc_info=True)
            self.METRICS.endTime = datetime.now(timezone.utc).isoformat()
            if quarantine_table:
                quarantine_table.restore_current_version()
            if self.raise_errors:
                raise
        finally:
            self.METRICS.endTime = datetime.now(timezone.utc).isoformat()
            self.ALLMETRICS.ingestion = self.METRICS.to_dict()

        return self

    def metrics(self, frmt: str = "json"):
        """
        Get the metrics that were generated during the ingestion process in json or dict format.
        """
        if frmt == "json":
            return self.ALLMETRICS.to_json()
        if frmt == "dict":
            return self.ALLMETRICS.to_dict()
        raise ValueError(f"Unsupported format: {frmt}")
        
    # --- Watermark Management ---
    def perform_pre_check(self, dataset_config_folder_name: str = "", dataset_file_name: str = "") -> None:
        """Perform pre-ingestion checks and update watermark if needed."""
        self.dataset_config_folder_name = dataset_config_folder_name
        self.dataset_file_name = dataset_file_name
        if self.dataset_type_name == "database" and self.ingest_type in ["watermark", "cdc"]:
            self.update_watermark_query()
        return self

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
            cdc_columns = ["__$operation", "__$start_lsn", "__$seqval"] 
            columnsList = f"{include_specific_columns},{','.join(cdc_columns)}" if include_specific_columns != "*" else "*"
            query = f"""
                WITH LatestChanges AS (
                    SELECT 
                        {columnsList},
                        ROW_NUMBER() OVER (
                            PARTITION BY {primary_keys}
                            ORDER BY [__$seqval] DESC
                        ) AS rn
                    FROM {table_name}
                    WHERE [__$operation] IN (1, 2, 4) AND __$start_lsn > {source_watermark_identifier_value}{filter_expression}
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
        file_location = f"{self._metadata_lakehouse.lakehouse_path}/Files/datasets/{self.dataset_config_folder_name}/watermark/{self.dataset_file_name}.json"
        notebookutils.fs.put(file_location, json.dumps(query_extract_json), overwrite=True)
        return self

    def _create_clustered_table(self, table_path: str):
        """Create Delta table with Liquid Clustering (max 4 columns)"""

        # Column definitions
        col_defs = [f"`{name}` {dtype}" for name, dtype in self._source_data.dtypes]
        col_defs_str = ", ".join(col_defs)

        # Build clustering columns safely (Delta supports max 4)
        base_cluster_cols = ["dl_lastmodifiedutc"]
        candidate_keys = self.candidate_keys or []

        # Preserve order, remove duplicates
        all_cluster_cols = []
        for col in base_cluster_cols + candidate_keys:
            if col not in all_cluster_cols:
                all_cluster_cols.append(col)

        # Enforce Delta limit
        max_cluster_cols = 4
        cluster_cols = all_cluster_cols[:max_cluster_cols]

        if len(all_cluster_cols) > max_cluster_cols:
            self.logger.warning(
                f"CLUSTER BY supports max {max_cluster_cols} columns. "
                f"Truncating clustering columns from {len(all_cluster_cols)} to {max_cluster_cols}: "
                f"{cluster_cols}"
            )

        cluster_clause = ", ".join(f"`{c}`" for c in cluster_cols)

        sql = f"""
            CREATE TABLE IF NOT EXISTS delta.`{table_path}` ({col_defs_str})
            CLUSTER BY ({cluster_clause})
        """

        spark.sql(sql)

        self.logger.info(
            f"Created clustered table with columns [{cluster_clause}]: "
            f"{self.target_schema}.{self.target_table}"
        )
