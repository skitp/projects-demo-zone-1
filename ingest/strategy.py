import logging
from typing import List, Optional, Dict
from datetime import datetime, timezone
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, row_number, lit
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from spark_engine.sparkconf import spark
from spark_engine.data_quality.quarantine import QuarantineTypes
from gdap_logging import GDAPLogging

logger = GDAPLogging(
    logger_name="strategy",
    logger_level=logging.INFO,
    root_log_level=logging.INFO,
    logging_format_spec="%(levelname)s: %(asctime)s: %(lineno)d: %(module)s: %(funcName)s: %(message)s",
).get_gdap_logger()


class LoadStrategy(ABC):
    def __init__(self) -> None:
        self.batch_time: Optional[str] = None
        self._candidate_keys: List[str] = []
        self._partition_keys: List[str] = []
        self.table_path: Optional[str] = None
        self.quarantine_metrics: Optional[Dict] = None

    def configure_load(
        self,
        table_path: str,
        candidate_keys: Optional[List[str]],
        partition_keys: Optional[List[str]],
        batch_time: Optional[datetime],
    ) -> None:
        logger.info("Configuring load for table: %s", table_path)
        if not batch_time:
            batch_time = datetime.now(timezone.utc)
        self.batch_time = batch_time.isoformat()
        self._candidate_keys = candidate_keys or []
        self._partition_keys = partition_keys or []
        self.table_path = table_path

    def first_time_load(self, source_data: DataFrame) -> None:
        logger.info("First-time load → %s", self.table_path)
        (
            source_data.write.format("delta")
            .partitionBy(self._partition_keys)
            .option("delta.checkpoint.writeStatsAsStruct", "true")
            .option("delta.checkpoint.writeStatsAsJson", "false")
            .save(self.table_path)
        )

    def deduplicate(self, source_data: DataFrame, quarantine_type: str) -> DataFrame:
        if not self._candidate_keys:
            logger.warning("No candidate keys – skipping deduplication")
            return source_data

        # Prefer CDC sequence columns when present so the latest event wins
        order_cols = ["dl_lastmodifiedutc"]
        if "__start_lsn" in source_data.columns:
            order_cols = ["__start_lsn", "__seqval"] if "__seqval" in source_data.columns else ["__start_lsn"]

        window = Window.partitionBy(*self._candidate_keys).orderBy(
            *[col(c).desc() for c in order_cols]
        )
        df = source_data.withColumn("_rn", row_number().over(window))
        unique = df.filter(col("_rn") == 1).drop("_rn")
        dups = df.filter(col("_rn") > 1)

        quarantine = QuarantineTypes.get_quarantine_type(quarantine_type)
        quarantine.configure(self.table_path)
        quarantine.quarantine(dups)
        self.quarantine_metrics = quarantine.quarantine_metrics
        logger.info("Deduplicated; quarantined %s rows", dups.count())
        return unique

    @staticmethod
    def _create_merge_predicate(keys: List[str]) -> str:
        # Null-safe equality – required when any PK column is nullable
        return " AND ".join(f"sd.{k} <=> bd.{k}" for k in keys)

    def get_table_metrics(self, table_path: str) -> tuple[int, int, int, int]:
        hist = (
            DeltaTable.forPath(spark, table_path)
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
        )
        inserts = updates = deletes = output_bytes = 0
        for row in hist.collect():
            op, ti, tu, td, ri, ru, rd, rb = row
            ti, tu, td = (int(x or 0) for x in (ti, tu, td))
            ri, ru, rd, rb = (int(x or 0) for x in (ri, ru, rd, rb))
            if op in {"CREATE TABLE AS SELECT", "WRITE", "CREATE OR REPLACE TABLE", "CREATE OR REPLACE TABLE AS SELECT"}:
                inserts += ri
                output_bytes += rb
            elif op == "MERGE":
                inserts += ti
                updates += tu
                deletes += td
            elif op == "UPDATE":
                updates += ru
            elif op == "DELETE":
                deletes += rd
        logger.info("Metrics → inserts=%s updates=%s deletes=%s bytes=%s", inserts, updates, deletes, output_bytes)
        return inserts, updates, deletes, output_bytes

    @abstractmethod
    def ingest(self, source_data: DataFrame) -> None:
        ...

    @staticmethod
    def _col_map(df: DataFrame, exclusions: List[str]) -> Dict[str, str]:
        return {c: f"sd.{c}" for c in df.columns if c not in exclusions}


class OverwriteLoad(LoadStrategy):
    def ingest(self, source_data: DataFrame) -> None:
        logger.info("Overwrite → %s", self.table_path)
        (
            source_data.write.format("delta")
            .partitionBy(self._partition_keys)
            .option("delta.checkpoint.writeStatsAsStruct", "true")
            .option("delta.checkpoint.writeStatsAsJson", "false")
            .option("overwriteSchema", "true")
            .mode("overwrite")
            .save(self.table_path)
        )


class AppendLoad(LoadStrategy):
    def ingest(self, source_data: DataFrame) -> None:
        logger.info("Append → %s", self.table_path)
        (
            source_data.write.format("delta")
            .partitionBy(self._partition_keys)
            .option("mergeSchema", "true")
            .mode("append")
            .save(self.table_path)
        )


class MergeLoad(LoadStrategy):
    _CDC_COLS = ["__start_lsn", "__seqval", "__operation"]

    def ingest(self, source_data: DataFrame) -> None:
        if not self._candidate_keys:
            raise ValueError("MergeLoad requires candidate_keys")

        logger.info("Merge → %s", self.table_path)
        base = DeltaTable.forPath(spark, self.table_path)
        predicate = self._create_merge_predicate(self._candidate_keys)
        is_cdc = "__operation" in source_data.columns

        if is_cdc:
            # Exclude CDC metadata from both update and insert so they never land in the target
            business_map = self._col_map(
                source_data,
                exclusions=["dl_createddateutc"] + self._CDC_COLS,
            )
            (
                base.alias("bd")
                .merge(source_data.alias("sd"), predicate)
                .withSchemaEvolution()
                .whenMatchedUpdate(
                    condition="sd.__operation IN (2, 4) AND bd.dl_rowhash <> sd.dl_rowhash",
                    set=business_map,
                )
                .whenMatchedUpdate(
                    condition="sd.__operation = 1",
                    set={
                        "dl_is_deleted": lit(1),
                        "dl_iscurrent": lit(0),
                        "dl_lastmodifiedutc": lit(self.batch_time),
                    },
                )
                .whenNotMatchedInsert(
                    condition="sd.__operation IN (2, 4)",
                    values=business_map,
                )
                .execute()
            )
            logger.info(
                "CDC merge finished (soft-delete). Compare active rows (dl_iscurrent=1) with source, not physical row count."
            )
        else:
            upd = self._col_map(source_data, exclusions=["dl_createddateutc"])
            (
                base.alias("bd")
                .merge(source_data.alias("sd"), predicate)
                .withSchemaEvolution()
                .whenMatchedUpdate("bd.dl_rowhash <> sd.dl_rowhash", set=upd)
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("Non-CDC merge finished")


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
        strategy_cls = getattr(cls.LoadStrategies, load_type, None)
        if strategy_cls is None:
            raise ValueError(f"Unknown load type: {load_type}")
        return strategy_cls()
