from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit, desc
from abc import ABC, abstractmethod
from delta.tables import DeltaTable
from datetime import datetime, timezone
from typing import List, Optional

from spark_engine.sparkconf import spark
from spark_engine.common.lakehouse import LakehouseManager
from spark_engine.data_quality.enums.quarantine_load_type import QuarantineLoadType

class QuarantineStrategy(ABC):
    def __init__(self, quarantine_type: str):
        self.quarantine_type = quarantine_type
        self.batch_time = datetime.now(timezone.utc).isoformat()

    def configure(
            self,
            table_path: str,
            index_column_names: Optional[List] = None,
            quarantine_strategy: str = None
        ):
        self.table_path = table_path
        self.quarantine_path = self.table_path + "_quarantine"
        self.quarantine_strategy = QuarantineLoadType(quarantine_strategy) if quarantine_strategy else None
        self.index_column_names = index_column_names

        if self.quarantine_strategy == QuarantineLoadType.MERGE and not self.index_column_names:
            raise ValueError(f"The index_column_names are missing for quarantine strategy '{self.quarantine_strategy.value}' and must be added to the quarantine config.")

        return self

    @abstractmethod
    def quarantine(self, source_data: DataFrame):
        raise NotImplementedError

    def append_quarantine_data(self, quarantine_data: DataFrame):
        options = {
            "delta.checkpoint.writeStatsAsStruct": "true",
            "delta.checkpoint.writeStatsAsJson": "false",
            "mergeSchema": "true"
        }
        quarantine_data.write.format("delta").mode("append").options(**options).save(
            self.quarantine_path
        )
        self.set_table_metrics()
        return self
    
    def overwrite_quarantine_data(self, quarantine_data: DataFrame):
        options = {
            "overwriteSchema": "true"
        }
        quarantine_data.write.format("delta").mode("overwrite").options(**options).save(
            self.quarantine_path
        )
        self.set_table_metrics()
        return self

    @staticmethod
    def create_merge_predicate(merge_predicate: list[str]) -> str:
        return " and ".join([f"source.{item} = target.{item}" for item in merge_predicate])
    
    def merge_quarantine_data(self, quarantine_data: DataFrame) -> None:
        if DeltaTable.isDeltaTable(spark, self.quarantine_path):
            deltaTableDest = DeltaTable.forPath(spark, self.quarantine_path)

            if "dl_row_hash" in quarantine_data.columns:
                update_predicate = "target.dl_row_hash <> source.dl_row_hash"
            elif "dl_rowhash" in quarantine_data.columns:
                update_predicate = "target.dl_rowhash <> source.dl_rowhash"
            else:
                update_predicate = None

            (
                deltaTableDest.alias("target")
                .merge(
                    quarantine_data.alias("source"),
                    self.create_merge_predicate(self.index_column_names)
                )
                .withSchemaEvolution()
                .whenMatchedUpdateAll(update_predicate)
                .whenNotMatchedInsertAll()
                .execute()
            )
        
        else:
            self.append_quarantine_data(quarantine_data)

        self.set_table_metrics()
        return self

    def set_table_metrics(self) -> tuple:
        df = DeltaTable.forPath(spark, self.quarantine_path).history().where(f"timestamp > '{self.batch_time}'")
        df = (
            df.selectExpr(
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
        for row in df.collect():
            (
                op,
                tar_inserts,
                tar_updates,
                tar_deletes,
                rec_inserts,
                rec_updates,
                rec_deletes,
                rec_bytes_output,
            ) = row
            if op in {
                "CREATE TABLE AS SELECT",
                "WRITE",
                "CREATE OR REPLACE TABLE",
                "CREATE OR REPLACE TABLE AS SELECT",
            }:
                inserts += int(rec_inserts)
                output_bytes += int(rec_bytes_output)
            elif op == "MERGE":
                inserts += int(tar_inserts)
                updates += int(tar_updates)
                deletes += int(tar_deletes)
            elif op == "UPDATE":
                updates += int(rec_updates)
            elif op == "DELETE":
                deletes += int(rec_deletes)

        self.quarantine_metrics = (
            {
                "quarantine_type": self.quarantine_type,
                "inserts": inserts,
                "updates": updates,
                "deletes": deletes,
                "output_bytes": output_bytes
            }
        ) if inserts + updates + deletes > 0 else None

    
class QuarantineVersion(QuarantineStrategy):
    def quarantine(self):
        super().quarantine(None)

    def set_current_version(self):
        table_exists = DeltaTable.isDeltaTable(spark, self.quarantine_path)
        if table_exists:
            df = DeltaTable.forPath(spark, self.quarantine_path).history()
            self.current_version = df.selectExpr("max(version) as version").collect()[0][0]
        else:
            self.current_version = None
        return self

    def restore_current_version(self):
        table_exists = DeltaTable.isDeltaTable(spark, self.quarantine_path)
        if table_exists:
            dt = DeltaTable.forPath(spark, self.quarantine_path)
            if self.current_version:
                dt.restoreToVersion(self.current_version)
            else:
                dt.delete()
        return self

class DataQuality(QuarantineStrategy):
    def quarantine(self, source_data: DataFrame):
        """
        Create quarantined dataframe.
        """
        self.quarantine_data = source_data
        self.quarantine_data = self.quarantine_data.withColumn(
            "dl_quarantine_type", lit(self.quarantine_type)
        )
        if self.quarantine_strategy == QuarantineLoadType.APPEND:
            self.append_quarantine_data(self.quarantine_data)
        elif self.quarantine_strategy == QuarantineLoadType.OVERWRITE:
            self.overwrite_quarantine_data(self.quarantine_data)
        else:
            self.merge_quarantine_data(self.quarantine_data)
    
class RetainAllDuplicate(QuarantineStrategy):
    def quarantine(self, source_data: DataFrame):
        """
        Create quarantined dataframe.
        """
        self.quarantine_data = source_data.filter(col("row_number") > 1).drop("row_number")
        self.quarantine_data = self.quarantine_data.withColumn(
            "dl_quarantine_type", lit(self.quarantine_type)
        )
        self.append_quarantine_data(self.quarantine_data)
    

class RetainOneDuplicate(QuarantineStrategy):
    def quarantine(self, source_data: DataFrame):
        """
        Create quarantined dataframe.
        """
        self.quarantine_data = source_data.filter(col("row_number") == 2).drop("row_number")
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
        quarantine_strategy = getattr(cls.QuarantineStrategies, quarantine_type)
        return quarantine_strategy(quarantine_type)

class QuarantineLog():
    LAKEHOUSE = "den_lhw_pdi_001_observability"
    SCHEMA = "audit"
    TABLE = "quarantine_log"

    def __init__(self):
        self.lakehouse = LakehouseManager(self.LAKEHOUSE)
        self.quarantine_df = None

    def add_log_data(self, **kwargs):
        df = spark.createDataFrame([kwargs])
        self.quarantine_df = self.quarantine_df.unionByName(df) if self.quarantine_df else df
        return self

    def write_log(self):
        assert self.lakehouse.check_if_table_exists(
            self.TABLE, self.SCHEMA
        ), f"Quarantine log table '{self.TABLE}' is missing."

        dtypes = self.lakehouse.get_table_dtypes(self.TABLE, self.SCHEMA)
        df = self.quarantine_df.withColumns({dt[0]: col(dt[0]).cast(dt[1]) for dt in dtypes if dt[0] in self.quarantine_df.columns})

        self.lakehouse.write_delta_table(
            df, self.SCHEMA, self.TABLE, None, "append"
        )
        return self


        
