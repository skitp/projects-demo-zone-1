import logging
from typing import List, Optional, Dict
from datetime import datetime, timezone
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, row_number, lit, trim
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from spark_engine.sparkconf import spark
from spark_engine.data_quality.quarantine import QuarantineTypes
from spark_engine.common.gdap_logging import GDAPLogging

logger = GDAPLogging(
    logger_name="strategy",
    logger_level=logging.INFO,
    root_log_level=logging.INFO,
    logging_format_spec="%(levelname)s: %(asctime)s: %(lineno)d: %(module)s: %(funcName)s: %(message)s",
).get_gdap_logger()


class LoadStrategy(ABC):
    """Abstract base class for load strategies."""

    def __init__(self) -> None:
        self.batch_time: Optional[str] = None
        self._candidate_keys: List[str] = []
        self._partition_keys: Optional[List[str]] = None
        self.table_path: Optional[str] = None
        self.quarantine_metrics: Optional[Dict] = None

    def configure_load(
        self,
        table_path: str,
        candidate_keys: Optional[List[str]],
        batch_time: Optional[datetime],
    ) -> None:
        logger.info("Configuring load for table: %s", table_path)
        self.batch_time = (batch_time or datetime.now(timezone.utc)).isoformat()
        self._candidate_keys = candidate_keys or []
        self.table_path = table_path
        logger.debug(
            "Configured candidate_keys=%s batch_time=%s",
            self._candidate_keys,
            self.batch_time,
        )

    def first_time_load(self, source_data: DataFrame) -> None:
        logger.info("First-time load → %s", self.table_path)
        try:
            (
                source_data.write.format("delta")
                .option("delta.checkpoint.writeStatsAsStruct", "true")
                .save(self.table_path)
            )
            logger.info("First-time load completed")
        except Exception as e:
            logger.error("Error during first-time load: %s", e)
            raise

    def deduplicate(self, source_data: DataFrame, quarantine_type: str) -> DataFrame:
        """Deduplicate by candidate keys; quarantine only when duplicates exist."""
        logger.info("Deduplicating (quarantine_type=%s)", quarantine_type)
        if not self._candidate_keys:
            logger.warning("No candidate keys; skipping deduplication")
            return source_data

        try:
            order_exprs = [col("dl_lastmodifiedutc").desc()]
            if "__start_lsn" in source_data.columns:
                order_exprs = [col("__start_lsn").desc()]
                if "__seqval" in source_data.columns:
                    order_exprs.append(col("__seqval").desc())
                if "__command_id" in source_data.columns:
                    order_exprs.append(col("__command_id").desc())

            window = Window.partitionBy(*self._candidate_keys).orderBy(*order_exprs)
            df = source_data.withColumn("row_number", row_number().over(window))
            unique_data = df.filter(col("row_number") == 1).drop("row_number")
            duplicates = df.filter(col("row_number") > 1)

            if duplicates.limit(1).count() > 0:
                quarantine = QuarantineTypes.get_quarantine_type(quarantine_type)
                quarantine.configure(self.table_path)
                quarantine.quarantine(duplicates)
                self.quarantine_metrics = quarantine.quarantine_metrics
                logger.info("Quarantined duplicate rows")
            else:
                logger.info("No duplicates; skipping quarantine")
                self.quarantine_metrics = None

            return unique_data
        except Exception as e:
            logger.error("Error during deduplication: %s", e)
            raise

    @staticmethod
    def _create_merge_predicate(keys: List[str]) -> str:
        """Null-safe equality so nullable candidate keys still match."""
        return " AND ".join(f"sd.{k} <=> bd.{k}" for k in keys)

    def _normalize_key_columns(self, df: DataFrame) -> DataFrame:
        """Trim string candidate keys to avoid whitespace mismatches."""
        if df is None or not self._candidate_keys:
            return df
        dtypes = dict(df.dtypes)
        for key in self._candidate_keys:
            if key not in df.columns:
                logger.warning("Candidate key '%s' missing during normalization", key)
                continue
            if "string" in dtypes.get(key, "").lower():
                df = df.withColumn(key, trim(col(key)))
        return df

    def _write_unmatched_keys(self, unmatched_df: DataFrame) -> None:
        """Write unmatched source keys to quarantine.<table>_unmatched."""
        if unmatched_df is None:
            return
        if hasattr(unmatched_df, "isEmpty") and unmatched_df.isEmpty():
            return

        table_name = self.table_path.rstrip("/").split("/")[-1]
        try:
            from spark_engine.data_quality.quarantine import QuarantineStrategy
            base = QuarantineStrategy._derive_quarantine_path(self.table_path)
            diag_path = base + "_unmatched"
        except Exception:
            parts = self.table_path.rstrip("/").split("/")
            diag_path = "/".join(parts[:-1]) + f"/quarantine/{table_name}_unmatched"

        keep = [c for c in self._candidate_keys if c in unmatched_df.columns]
        for extra in ("__operation", "__start_lsn", "__seqval", "__command_id", "dl_rowhash"):
            if extra in unmatched_df.columns and extra not in keep:
                keep.append(extra)

        out = (
            unmatched_df.select(*keep)
            .withColumn("dl_diagnostic_type", lit("unmatched_source_key"))
            .withColumn("dl_batch_time", lit(str(self.batch_time)))
            .withColumn("dl_captured_utc", lit(datetime.now(timezone.utc).isoformat()))
        )
        logger.warning("Writing unmatched keys to %s", diag_path)
        (
            out.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("delta.checkpoint.writeStatsAsStruct", "true")
            .save(diag_path)
        )

    def get_table_metrics(self, table_path: str) -> tuple:
        logger.info("Fetching metrics for %s after %s", table_path, self.batch_time)
        try:
            history_df = DeltaTable.forPath(spark, table_path).history()
            if self.batch_time and str(self.batch_time) != "None":
                history_df = history_df.where(f"timestamp > '{self.batch_time}'")
            else:
                history_df = history_df.where(
                    "timestamp >= current_timestamp() - interval 15 minutes"
                )

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
                ti = int(row["numTargetRowsInserted"] or 0)
                tu = int(row["numTargetRowsUpdated"] or 0)
                td = int(row["numTargetRowsDeleted"] or 0)
                ri = int(row["numOutputRows"] or 0)
                ru = int(row["numUpdatedRows"] or 0)
                rd = int(row["numDeletedRows"] or 0)
                rb = int(row["numOutputBytes"] or 0)

                if op in {
                    "CREATE TABLE",
                    "CREATE TABLE AS SELECT",
                    "WRITE",
                    "CREATE OR REPLACE TABLE",
                    "CREATE OR REPLACE TABLE AS SELECT",
                }:
                    inserts += ti or ri
                    output_bytes += rb
                elif op == "MERGE":
                    inserts += ti
                    updates += tu
                    deletes += td
                elif op == "UPDATE":
                    updates += ru
                elif op == "DELETE":
                    deletes += rd

            logger.info(
                "Metrics inserts=%s updates=%s deletes=%s bytes=%s",
                inserts, updates, deletes, output_bytes,
            )
            return inserts, updates, deletes, output_bytes
        except Exception as e:
            logger.error("Failed to fetch metrics: %s", e)
            raise

    @abstractmethod
    def ingest(self, source_data: DataFrame) -> None:
        raise NotImplementedError

    @staticmethod
    def _get_upd_col(df: DataFrame, exclusions: List[str]) -> Dict[str, str]:
        return {c: f"sd.{c}" for c in df.columns if c not in exclusions}


class OverwriteLoad(LoadStrategy):
    def ingest(self, source_data: DataFrame) -> None:
        logger.info("Overwrite → %s", self.table_path)
        try:
            (
                source_data.write.format("delta")
                .option("delta.checkpoint.writeStatsAsStruct", "true")
                .option("overwriteSchema", "true")
                .mode("overwrite")
                .save(self.table_path)
            )
            logger.info("Overwrite completed")
        except Exception as e:
            logger.error("Error during overwrite: %s", e)
            raise


class AppendLoad(LoadStrategy):
    def ingest(self, source_data: DataFrame) -> None:
        logger.info("Append → %s", self.table_path)
        try:
            (
                source_data.write.format("delta")
                .option("mergeSchema", "true")
                .mode("append")
                .save(self.table_path)
            )
            logger.info("Append completed")
        except Exception as e:
            logger.error("Error during append: %s", e)
            raise


class MergeLoad(LoadStrategy):
    """
    Unified CDC merge path.

    CDC source must contain operations 1, 2, 3 and 4 (latest per key).
    - Latest op 1 or 3 → soft-delete the key
    - Latest op 2 or 4 → upsert the key (reactivates if previously soft-deleted)

    Non-CDC merge is unchanged.
    """

    def ingest(self, source_data: DataFrame) -> None:
        logger.info("Merging into %s", self.table_path)
        if not self._candidate_keys:
            logger.warning("No candidate keys; skipping merge")
            return

        try:
            source_data = self._normalize_key_columns(source_data)
            base_data = DeltaTable.forPath(spark, self.table_path)
            merge_predicate = self._create_merge_predicate(self._candidate_keys)
            logger.info("Merge predicate: %s", merge_predicate)

            is_cdc = "__operation" in source_data.columns
            if is_cdc:
                self._ingest_cdc(source_data, base_data, merge_predicate)
            else:
                self._ingest_non_cdc(source_data, base_data, merge_predicate)

            logger.info("Merge completed")
        except Exception as e:
            logger.error("Error during merge ingest: %s", e)
            raise

    def _ingest_cdc(self, source_data: DataFrame, base_data, merge_predicate: str) -> None:
        """Single CDC path for all tables (ops 1/2/3/4)."""
        # Latest operation per key
        order_cols = [
            c for c in ("__start_lsn", "__command_id", "__seqval")
            if c in source_data.columns
        ]
        if order_cols:
            w = Window.partitionBy(*self._candidate_keys).orderBy(
                *[col(c).desc() for c in order_cols]
            )
            latest = (
                source_data.withColumn("_rn", row_number().over(w))
                .filter(col("_rn") == 1)
                .drop("_rn")
            )
        else:
            latest = source_data
            logger.warning("No CDC sequence columns; using source as-is for latest")

        # Diagnostic: unmatched keys (before any merge)
        self._log_unmatched(latest)

        to_delete = latest.filter(col("__operation").isin(1, 3))
        to_upsert = latest.filter(col("__operation").isin(2, 4))

        # 1. Soft-delete (deletes + before-images of key updates)
        if to_delete.limit(1).count() > 0:
            logger.info("CDC soft-delete phase")
            (
                base_data.alias("bd")
                .merge(to_delete.alias("sd"), merge_predicate)
                .whenMatchedUpdate(
                    set={
                        "dl_isdeleted": lit(1),
                        "dl_iscurrent": lit(0),
                        "dl_lastmodifiedutc": lit(self.batch_time),
                    }
                )
                .execute()
            )

        # 2. Upsert (inserts + after-images of key updates)
        if to_upsert.limit(1).count() > 0:
            logger.info("CDC upsert phase")
            upd = self._get_upd_col(
                to_upsert,
                exclusions=[
                    "dl_createddateutc",
                    "__start_lsn",
                    "__seqval",
                    "__command_id",
                    "__operation",
                ],
            )
            ins = self._get_upd_col(
                to_upsert,
                exclusions=["__operation", "__start_lsn", "__seqval", "__command_id"],
            )
            upd = {
                **upd,
                "dl_isdeleted": lit(0),
                "dl_iscurrent": lit(1),
                "dl_lastmodifiedutc": lit(self.batch_time),
            }
            ins = {**ins, "dl_isdeleted": lit(0), "dl_iscurrent": lit(1)}

            (
                base_data.alias("bd")
                .merge(to_upsert.alias("sd"), merge_predicate)
                .withSchemaEvolution()
                .whenMatchedUpdate(set=upd)
                .whenNotMatchedInsert(values=ins)
                .execute()
            )

        logger.info(
            "CDC merge finished. Compare active rows (dl_iscurrent=1) with source."
        )

    def _log_unmatched(self, source_data: DataFrame) -> None:
        key_cols = [c for c in self._candidate_keys if c in source_data.columns]
        if not key_cols:
            return
        try:
            target_keys = (
                spark.read.format("delta")
                .load(self.table_path)
                .select(*key_cols)
            )
            target_keys = self._normalize_key_columns(target_keys)
            unmatched = source_data.join(target_keys, on=key_cols, how="left_anti")
            if unmatched.limit(1).count() > 0:
                logger.warning("Unmatched source keys detected")
                self._write_unmatched_keys(unmatched)
        except Exception as e:
            logger.warning("Unmatched-key diagnostic skipped: %s", e)

    def _ingest_non_cdc(self, source_data: DataFrame, base_data, merge_predicate: str) -> None:
        upd = self._get_upd_col(source_data, exclusions=["dl_createddateutc"])
        row_changed = "NOT (bd.dl_rowhash <=> sd.dl_rowhash)"
        (
            base_data.alias("bd")
            .merge(source_data.alias("sd"), merge_predicate)
            .withSchemaEvolution()
            .whenMatchedUpdate(condition=row_changed, set=upd)
            .whenNotMatchedInsertAll()
            .execute()
        )


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
            logger.error("Invalid load type: %s", load_type)
            raise ValueError(f"Invalid load type: {load_type}")
        return strategy_cls()
