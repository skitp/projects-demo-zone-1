import logging
from typing import List, Optional
from datetime import datetime, timezone
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, desc
from delta.tables import DeltaTable

from spark_engine.sparkconf import spark
from spark_engine.common.lakehouse import LakehouseManager
from spark_engine.common.gdap_logging import GDAPLogging
from spark_engine.data_quality.enums.quarantine_load_type import QuarantineLoadType

logger = GDAPLogging(
    logger_name="quarantine",
    logger_level=logging.INFO,
    root_log_level=logging.INFO,
    logging_format_spec="%(levelname)s: %(asctime)s: %(lineno)d: %(module)s: %(funcName)s: %(message)s",
).get_gdap_logger()


class QuarantineStrategy(ABC):
    """Abstract base for quarantine strategies."""

    def __init__(self, quarantine_type: str):
        self.quarantine_type = quarantine_type
        self.batch_time = datetime.now(timezone.utc).isoformat()
        self.quarantine_metrics = None

    def configure(
        self,
        table_path: str,
        index_column_names: Optional[List[str]] = None,
        quarantine_strategy: Optional[str] = None,
    ):
        """Configure quarantine target under schema 'quarantine'."""
        self.table_path = table_path
        self.quarantine_path = self._derive_quarantine_path(table_path)
        self.quarantine_strategy = (
            QuarantineLoadType(quarantine_strategy) if quarantine_strategy else None
        )
        self.index_column_names = index_column_names or []

        if (
            self.quarantine_strategy == QuarantineLoadType.MERGE
            and not self.index_column_names
        ):
            raise ValueError(
                f"index_column_names are required for quarantine strategy "
                f"'{self.quarantine_strategy.value}'"
            )

        logger.info(
            "Quarantine configured: type=%s path=%s strategy=%s",
            self.quarantine_type,
            self.quarantine_path,
            self.quarantine_strategy,
        )
        return self

    @staticmethod
    def _derive_quarantine_path(table_path: str) -> str:
        """
        Derive path for quarantine.<table_name>.

        Examples:
          .../Tables/dbo/my_table          -> .../Tables/quarantine/my_table
          .../Tables/my_table              -> .../Tables/quarantine/my_table
          .../Tables/schema/sub/my_table   -> .../Tables/quarantine/my_table
        """
        path = table_path.rstrip("/")
        parts = path.split("/")

        try:
            tables_idx = next(i for i, p in enumerate(parts) if p.lower() == "tables")
        except StopIteration:
            # Fallback: treat last segment as table name
            table_name = parts[-1]
            return "/".join(parts[:-1]) + f"/quarantine/{table_name}"

        after = parts[tables_idx + 1 :]
        if not after:
            raise ValueError(f"Invalid table path (no table name after Tables): {table_path}")

        table_name = after[-1]
        new_parts = parts[: tables_idx + 1] + ["quarantine", table_name]
        return "/".join(new_parts)

    @abstractmethod
    def quarantine(self, source_data: DataFrame):
        raise NotImplementedError

    def _has_rows(self, df: DataFrame) -> bool:
        """Cheap emptiness check (avoids full count when possible)."""
        if df is None:
            return False
        # Prefer isEmpty when available (Spark 3.3+)
        if hasattr(df, "isEmpty"):
            return not df.isEmpty()
        return df.limit(1).count() > 0

    def append_quarantine_data(self, quarantine_data: DataFrame):
        if not self._has_rows(quarantine_data):
            logger.info("No rows to append to quarantine; skipping write")
            self.quarantine_metrics = None
            return self

        logger.info("Appending quarantine data to %s", self.quarantine_path)
        options = {
            "delta.checkpoint.writeStatsAsStruct": "true",
            "delta.checkpoint.writeStatsAsJson": "false",
            "mergeSchema": "true",
        }
        try:
            (
                quarantine_data.write.format("delta")
                .mode("append")
                .options(**options)
                .save(self.quarantine_path)
            )
            self.set_table_metrics()
        except Exception as e:
            logger.error("Failed to append quarantine data: %s", e)
            raise
        return self

    def overwrite_quarantine_data(self, quarantine_data: DataFrame):
        if not self._has_rows(quarantine_data):
            logger.info("No rows to overwrite quarantine; skipping write")
            self.quarantine_metrics = None
            return self

        logger.info("Overwriting quarantine data at %s", self.quarantine_path)
        options = {"overwriteSchema": "true"}
        try:
            (
                quarantine_data.write.format("delta")
                .mode("overwrite")
                .options(**options)
                .save(self.quarantine_path)
            )
            self.set_table_metrics()
        except Exception as e:
            logger.error("Failed to overwrite quarantine data: %s", e)
            raise
        return self

    @staticmethod
    def create_merge_predicate(merge_predicate: List[str]) -> str:
        """Null-safe equality so nullable index columns still match."""
        return " AND ".join(f"source.{item} <=> target.{item}" for item in merge_predicate)

    def merge_quarantine_data(self, quarantine_data: DataFrame):
        if not self._has_rows(quarantine_data):
            logger.info("No rows to merge into quarantine; skipping")
            self.quarantine_metrics = None
            return self

        logger.info("Merging quarantine data into %s", self.quarantine_path)
        try:
            if DeltaTable.isDeltaTable(spark, self.quarantine_path):
                delta_dest = DeltaTable.forPath(spark, self.quarantine_path)

                if "dl_row_hash" in quarantine_data.columns:
                    update_predicate = "NOT (target.dl_row_hash <=> source.dl_row_hash)"
                elif "dl_rowhash" in quarantine_data.columns:
                    update_predicate = "NOT (target.dl_rowhash <=> source.dl_rowhash)"
                else:
                    update_predicate = None

                (
                    delta_dest.alias("target")
                    .merge(
                        quarantine_data.alias("source"),
                        self.create_merge_predicate(self.index_column_names),
                    )
                    .withSchemaEvolution()
                    .whenMatchedUpdateAll(update_predicate)
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                self.append_quarantine_data(quarantine_data)
                return self

            self.set_table_metrics()
        except Exception as e:
            logger.error("Failed to merge quarantine data: %s", e)
            raise
        return self

    def set_table_metrics(self):
        """Collect metrics from the most recent operation after batch_time."""
        try:
            hist = (
                DeltaTable.forPath(spark, self.quarantine_path)
                .history()
                .where(f"timestamp > '{self.batch_time}'")
                .selectExpr(
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
                .limit(1)
            )

            inserts = updates = deletes = output_bytes = 0
            rows = hist.collect()
            if rows:
                row = rows[0]
                op = row["operation"]
                tar_inserts = int(row["numTargetRowsInserted"] or 0)
                tar_updates = int(row["numTargetRowsUpdated"] or 0)
                tar_deletes = int(row["numTargetRowsDeleted"] or 0)
                rec_inserts = int(row["numOutputRows"] or 0)
                rec_updates = int(row["numUpdatedRows"] or 0)
                rec_deletes = int(row["numDeletedRows"] or 0)
                rec_bytes = int(row["numOutputBytes"] or 0)

                if op in {
                    "CREATE TABLE AS SELECT",
                    "WRITE",
                    "CREATE OR REPLACE TABLE",
                    "CREATE OR REPLACE TABLE AS SELECT",
                    "CREATE TABLE",
                }:
                    inserts = tar_inserts or rec_inserts
                    output_bytes = rec_bytes
                elif op == "MERGE":
                    inserts = tar_inserts
                    updates = tar_updates
                    deletes = tar_deletes
                elif op == "UPDATE":
                    updates = rec_updates
                elif op == "DELETE":
                    deletes = rec_deletes

            self.quarantine_metrics = (
                {
                    "quarantine_type": self.quarantine_type,
                    "inserts": inserts,
                    "updates": updates,
                    "deletes": deletes,
                    "output_bytes": output_bytes,
                    "quarantine_path": self.quarantine_path,
                }
                if inserts + updates + deletes > 0
                else None
            )
            logger.info("Quarantine metrics: %s", self.quarantine_metrics)
        except Exception as e:
            logger.warning("Could not collect quarantine metrics: %s", e)
            self.quarantine_metrics = None


class QuarantineVersion(QuarantineStrategy):
    def quarantine(self, source_data: DataFrame = None):
        # Version-only strategy; no data write
        return self

    def set_current_version(self):
        if DeltaTable.isDeltaTable(spark, self.quarantine_path):
            df = DeltaTable.forPath(spark, self.quarantine_path).history()
            self.current_version = df.selectExpr("max(version) as version").collect()[0][0]
        else:
            self.current_version = None
        return self

    def restore_current_version(self):
        if DeltaTable.isDeltaTable(spark, self.quarantine_path):
            dt = DeltaTable.forPath(spark, self.quarantine_path)
            if getattr(self, "current_version", None) is not None:
                dt.restoreToVersion(self.current_version)
            else:
                dt.delete()
        return self


class DataQuality(QuarantineStrategy):
    def quarantine(self, source_data: DataFrame):
        if not self._has_rows(source_data):
            logger.info("No data-quality rows to quarantine")
            self.quarantine_metrics = None
            return self

        self.quarantine_data = source_data.withColumn(
            "dl_quarantine_type", lit(self.quarantine_type)
        )

        if self.quarantine_strategy == QuarantineLoadType.APPEND:
            self.append_quarantine_data(self.quarantine_data)
        elif self.quarantine_strategy == QuarantineLoadType.OVERWRITE:
            self.overwrite_quarantine_data(self.quarantine_data)
        else:
            self.merge_quarantine_data(self.quarantine_data)
        return self


class RetainAllDuplicate(QuarantineStrategy):
    def quarantine(self, source_data: DataFrame):
        # Expects source already containing row_number or filters itself
        if "row_number" in source_data.columns:
            self.quarantine_data = source_data.filter(col("row_number") > 1).drop("row_number")
        else:
            self.quarantine_data = source_data

        if not self._has_rows(self.quarantine_data):
            logger.info("No duplicate rows to retain")
            self.quarantine_metrics = None
            return self

        self.quarantine_data = self.quarantine_data.withColumn(
            "dl_quarantine_type", lit(self.quarantine_type)
        )
        self.append_quarantine_data(self.quarantine_data)
        return self


class RetainOneDuplicate(QuarantineStrategy):
    def quarantine(self, source_data: DataFrame):
        if "row_number" in source_data.columns:
            self.quarantine_data = source_data.filter(col("row_number") == 2).drop("row_number")
        else:
            self.quarantine_data = source_data.limit(0)  # nothing if no row_number

        if not self._has_rows(self.quarantine_data):
            logger.info("No single-duplicate rows to retain")
            self.quarantine_metrics = None
            return self

        self.quarantine_data = self.quarantine_data.withColumn(
            "dl_quarantine_type", lit(self.quarantine_type)
        )
        self.append_quarantine_data(self.quarantine_data)
        return self


class QuarantineTypes:
    class QuarantineStrategies:
        RETAIN_ONE_DUPLICATE = RetainOneDuplicate
        RETAIN_ALL_DUPLICATE = RetainAllDuplicate
        DATA_QUALITY = DataQuality

    @classmethod
    def get_quarantine_type(cls, quarantine_type: str):
        quarantine_type = quarantine_type.upper()
        strategy_cls = getattr(cls.QuarantineStrategies, quarantine_type, None)
        if strategy_cls is None:
            raise ValueError(f"Unknown quarantine type: {quarantine_type}")
        return strategy_cls(quarantine_type)


class QuarantineLog:
    LAKEHOUSE = "den_lhw_pdi_001_observability"
    SCHEMA = "audit"
    TABLE = "quarantine_log"

    def __init__(self):
        self.lakehouse = LakehouseManager(self.LAKEHOUSE)
        self.quarantine_df = None

    def add_log_data(self, **kwargs):
        df = spark.createDataFrame([kwargs])
        self.quarantine_df = (
            self.quarantine_df.unionByName(df) if self.quarantine_df else df
        )
        return self

    def write_log(self):
        if self.quarantine_df is None or not self._has_rows(self.quarantine_df):
            logger.info("No quarantine log rows to write")
            return self

        assert self.lakehouse.check_if_table_exists(
            self.TABLE, self.SCHEMA
        ), f"Quarantine log table '{self.TABLE}' is missing."

        dtypes = self.lakehouse.get_table_dtypes(self.TABLE, self.SCHEMA)
        df = self.quarantine_df.withColumns(
            {
                dt[0]: col(dt[0]).cast(dt[1])
                for dt in dtypes
                if dt[0] in self.quarantine_df.columns
            }
        )

        self.lakehouse.write_delta_table(df, self.SCHEMA, self.TABLE, None, "append")
        logger.info("Wrote quarantine log to %s.%s", self.SCHEMA, self.TABLE)
        return self

    @staticmethod
    def _has_rows(df: DataFrame) -> bool:
        if df is None:
            return False
        if hasattr(df, "isEmpty"):
            return not df.isEmpty()
        return df.limit(1).count() > 0
