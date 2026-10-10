import logging
from typing import List, Optional, Dict
from datetime import datetime, timezone
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, desc, row_number, lit
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from spark_engine.sparkconf import spark
from spark_engine.data_quality.quarantine import QuarantineTypes
from spark_engine.common.gdap_logging import GDAPLogging

# Initialize logger at module level for production-ready logging
logger = GDAPLogging(
    logger_name="strategy",
    logger_level=logging.INFO,  # Set to INFO for production; can be DEBUG for development
    root_log_level=logging.INFO,
    logging_format_spec='%(levelname)s: %(asctime)s: %(lineno)d: %(module)s: %(funcName)s: %(message)s'
).get_gdap_logger()

class LoadStrategy(ABC):
    """
    Abstract base class for load strategies.
    """

    def __init__(self) -> None:
        self.batch_time: Optional[datetime] = None
        self._candidate_keys: Optional[List[str]] = None
        self._partition_keys: Optional[List[str]] = None
        self.table_path: Optional[str] = None
        self._dataframe_condition: str = ""
        self.quarantine_metrics: Optional[Dict] = None  # Added for better type hinting

    def configure_load(
        self,
        table_path: str,
        candidate_keys: Optional[List[str]],
        batch_time: Optional[datetime],
    ) -> None:
        """
        Configures the load parameters.
        """
        logger.info(f"Configuring load for table: {table_path}")

        self.batch_time = batch_time or datetime.now(timezone.utc).isoformat()
        self._candidate_keys = candidate_keys or []
        self.table_path = table_path
        
        logger.debug(f"Configured with candidate_keys: {self._candidate_keys}, partition_keys: {self._partition_keys}, batch_time: {self.batch_time}")

    def first_time_load(self, source_data: DataFrame) -> None:
        """
        Performs the first-time load by writing the DataFrame as a Delta table.
        """
        logger.info(f"Performing first-time load to: {self.table_path}")
        try:
            source_data.write.format("delta") \
                .option("delta.checkpoint.writeStatsAsStruct", "true") \
                .save(self.table_path)
            logger.info("First-time load completed successfully")
        except Exception as e:
            logger.error(f"Error during first-time load: {str(e)}")
            raise

    def deduplicate(self, source_data: DataFrame, quarantine_type: str) -> DataFrame:
        """
        Deduplicates the source data based on candidate keys and quarantines duplicates.
        """
        logger.info(f"Deduplicating data with quarantine_type: {quarantine_type}")
        if not self._candidate_keys:
            logger.warning("No candidate keys provided; skipping deduplication")
            return source_data

        try:
            window = Window.partitionBy(*self._candidate_keys).orderBy(col("dl_lastmodifiedutc").desc())
            df = source_data.withColumn("row_number", row_number().over(window))
            unique_data = df.filter(col("row_number") == 1).drop("row_number")
            duplicates = df.filter(col("row_number") > 1)

            quarantine = QuarantineTypes.get_quarantine_type(quarantine_type)
            quarantine.configure(self.table_path)
            quarantine.quarantine(duplicates)
            self.quarantine_metrics = quarantine.quarantine_metrics
            logger.info(f"Deduplication completed. Quarantined {duplicates.count()} records.")
            return unique_data
        except Exception as e:
            logger.error(f"Error during deduplication: {str(e)}")
            raise

    @staticmethod
    def _create_merge_predicate(merge_predicate: List[str]) -> str:
        """
        Creates a null-safe merge predicate string from the list of keys.
        """
        return " and ".join([f"sd.{item} <=> bd.{item}" for item in merge_predicate])

    def get_table_metrics(self, table_path: str) -> tuple[int, int, int, int]:
        """
        Retrieves metrics from Delta table history after the batch time.
        Added logic to work for first-time loads and clusterd tables.
        """
        logger.info(f"Fetching metrics for table: {table_path} after batch_time: {self.batch_time}")
        try:
            delta_table = DeltaTable.forPath(spark, table_path)
            history_df = delta_table.history()
            
            # More reliable filtering
            if self.batch_time and str(self.batch_time) != "None":
                # Normal case - filter by batch start time
                history_df = history_df.where(f"timestamp > '{self.batch_time}'")
            else:
                # First-time load fallback - look at the last 15 minutes
                history_df = history_df.where("timestamp >= current_timestamp() - interval 15 minutes")

            history_df = (
                history_df.selectExpr(
                    "operation",
                    "operationMetrics.numTargetRowsInserted",
                    "operationMetrics.numTargetRowsUpdated",
                    "operationMetrics.numTargetRowsDeleted",
                    "operationMetrics.numOutputRows",
                    "operationMetrics.numUpdatedRows",
                    "operationMetrics.numDeletedRows",
                    "operationMetrics.numOutputBytes",
                )
                .orderBy(desc("timestamp"))
            )
            inserts = updates = deletes = output_bytes = 0
            for row in history_df.collect():
                op = row["operation"]
                tar_inserts = row["numTargetRowsInserted"] or 0
                tar_updates = row["numTargetRowsUpdated"] or 0
                tar_deletes = row["numTargetRowsDeleted"] or 0
                rec_inserts = row["numOutputRows"] or 0
                rec_updates = row["numUpdatedRows"] or 0
                rec_deletes = row["numDeletedRows"] or 0
                rec_bytes = row["numOutputBytes"] or 0

                if op in {"CREATE TABLE", "CREATE TABLE AS SELECT", "WRITE", 
                      "CREATE OR REPLACE TABLE", "CREATE OR REPLACE TABLE AS SELECT"}:
                    inserts += int(tar_inserts or rec_inserts)
                    output_bytes += int(rec_bytes)
                elif op == "MERGE":
                    inserts += int(tar_inserts)
                    updates += int(tar_updates)
                    deletes += int(tar_deletes)
                elif op == "UPDATE":
                    updates += int(rec_updates)
                elif op == "DELETE":
                    deletes += int(rec_deletes)

            logger.info(f"Metrics: inserts={inserts}, updates={updates}, deletes={deletes}, output_bytes={output_bytes}")
            return inserts, updates, deletes, output_bytes
        except Exception as e:
            logger.error(f"Failed to fetch Delta history metrics: {e}.")
            raise

    @abstractmethod
    def ingest(self, source_data: DataFrame) -> None:
        raise NotImplementedError

    @staticmethod
    def _get_upd_col(df: DataFrame, exclusions: List[str]) -> Dict[str, str]:
        """
        Gets update columns excluding specified ones.
        """
        return {i: "sd." + i for i in df.columns if i not in exclusions}

class OverwriteLoad(LoadStrategy):
    def ingest(self, source_data: DataFrame) -> None:
        """
        Ingests data by overwriting the existing table.
        """
        logger.info(f"Overwriting data in: {self.table_path}")
        try:
            source_data.write.format("delta") \
                .option("delta.checkpoint.writeStatsAsStruct", "true") \
                .option("overwriteSchema", "true") \
                .mode("overwrite") \
                .save(self.table_path)
            logger.info("Overwrite ingest completed successfully")
        except Exception as e:
            logger.error(f"Error during overwrite ingest: {str(e)}")
            raise

class AppendLoad(LoadStrategy):
    def ingest(self, source_data: DataFrame) -> None:
        """
        Ingests data by appending to the existing table.
        """
        logger.info(f"Appending data to: {self.table_path}")
        try:
            source_data.write.format("delta") \
                .option("mergeSchema", "true") \
                .mode("append") \
                .save(self.table_path)
            logger.info("Append ingest completed successfully")
        except Exception as e:
            logger.error(f"Error during append ingest: {str(e)}")
            raise

class MergeLoad(LoadStrategy):
    def ingest(self, source_data: DataFrame) -> None:
        """
        Ingests data by merging into the existing table.
        """
        logger.info(f"Merging data into: {self.table_path}")
        if not self._candidate_keys:
            logger.warning("No candidate keys provided; skipping merge")
            return

        try:
            base_data = DeltaTable.forPath(spark, self.table_path)
            merge_predicate = self._create_merge_predicate(self._candidate_keys)
            upd_col = self._get_upd_col(source_data, exclusions=["dl_createddateutc"])

            # Check if source data contains CDC columns
            is_cdc = "__operation" in source_data.columns
            logger.debug(f"CDC detected: {is_cdc}")

            # Null-safe "different hash" condition
            row_changed_condition = (
                "NOT (bd.dl_rowhash <=> sd.dl_rowhash)"
            )

            # Handle CDC: inserts (__operation = 2), updates (__operation = 4), and deletes (__operation = 1)
            if is_cdc:
                upd_col_cdc = self._get_upd_col(source_data, exclusions=["dl_createddateutc", "__start_lsn", "__seqval", "__operation"])
                ins_col_cdc = self._get_upd_col(df=source_data, exclusions=["__operation", "__start_lsn", "__seqval"])
                (
                    base_data.alias("bd").merge(
                        source=source_data.alias("sd"),
                        condition=merge_predicate
                    )
                    .withSchemaEvolution()
                    .whenMatchedUpdate(
                        condition=(
                        "sd.__operation IN (2, 4) "
                        f"AND {row_changed_condition}"
                    ),
                    set=upd_col_cdc
                    )
                    .whenMatchedUpdate(
                        condition="sd.__operation = 1",
                        set={
                            "dl_isdeleted": lit(1),
                            "dl_iscurrent": lit(0),
                            "dl_lastmodifiedutc": lit(self.batch_time)
                        }
                    )
                    .whenNotMatchedInsert(
                        condition="sd.__operation IN (2, 4)",
                        values=ins_col_cdc
                    )
                    .execute()
                )
            else:
                (
                    base_data.alias("bd").merge(
                        source=source_data.alias("sd"),
                        condition=merge_predicate
                    )
                    .withSchemaEvolution()
                    .whenMatchedUpdate(condition=row_changed_condition, set=upd_col)
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            logger.info("Merge ingest completed successfully")
        except Exception as e:
            logger.error(f"Error during merge ingest: {str(e)}")
            raise

class LoadTypes:
    class LoadStrategies:
        OVERWRITE_LOAD = OverwriteLoad
        APPEND_LOAD = AppendLoad
        MERGE_LOAD = MergeLoad

    @classmethod
    def get_load_type(cls, load_type: str) -> LoadStrategy:
        load_type = load_type.upper()
        if not load_type.endswith("_LOAD"):
            load_type += "_LOAD"
        load_strategy_class = getattr(cls.LoadStrategies, load_type, None)
        if load_strategy_class is None:
            logger.error(f"Invalid load type: {load_type}")
            raise ValueError(f"Invalid load type: {load_type}")
        return load_strategy_class()
