from typing import List, Optional
from datetime import datetime, timezone
from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, expr, sha2, concat_ws, lit, desc
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_engine.sparkconf import spark
from spark_engine.common.lakehouse import LakehouseManager
from spark_engine.transform.metadata import Metadata


RESERVED_UNKNOWN = -1
RESERVED_NOT_APPLICABLE = -2
IDENTITY_FLOOR = 0


class IdentityKeyMap:
    """Durable hash → bigint identity map backed by a Delta table.

    Map table: {schema}.map_{table}_identity
    Lookup key: SHA2-256 of normalized candidate keys
    Published key on dim/fact: sequential bigint (Power BI integer join)
    Identity is minted once per business key and never updated.
    """

    def __init__(
        self,
        lakehouse,
        schema: str,
        dimension_table: str,
        identity_column_name: str,
        candidate_keys: List[str],
        elt_id: str,
        run_id: str,
        batch_time,
        map_table: Optional[str] = None,
        map_schema: Optional[str] = None,
    ) -> None:
        if not candidate_keys:
            raise ValueError("candidate_keys must be defined when identity=True")

        self.lakehouse = lakehouse
        self.schema = schema
        self.dimension_table = dimension_table
        self.identity_col = identity_column_name
        self.candidate_keys = list(candidate_keys)
        self.elt_id = elt_id
        self.run_id = run_id
        self.batch_time = batch_time

        self.map_schema = map_schema or schema
        self.map_table = map_table or f"map_{dimension_table}_identity"
        self.map_exists = lakehouse.check_if_table_exists(self.map_table, self.map_schema)
        self.map_path = lakehouse.build_delta_file_path(self.map_table, self.map_schema)

    def bk_hash_expr(self, alias_prefix: Optional[str] = None):
        cols = []
        for name in self.candidate_keys:
            c = F.col(f"{alias_prefix}.{name}") if alias_prefix else F.col(name)
            cols.append(F.coalesce(c.cast("string"), F.lit("")))
        return F.sha2(F.concat_ws("||", *cols), 256)

    def with_bk_hash(self, df: DataFrame, col_name: str = "bk_hash") -> DataFrame:
        return df.withColumn(col_name, self.bk_hash_expr())

    def _read_map(self) -> DataFrame:
        return spark.read.format("delta").load(self.map_path)

    def _max_identity(self) -> int:
        if not self.map_exists:
            return IDENTITY_FLOOR
        val = (
            self._read_map()
            .select(F.coalesce(F.max(F.col(self.identity_col)), F.lit(IDENTITY_FLOOR)).alias("m"))
            .collect()[0]["m"]
        )
        return int(val) if val is not None else IDENTITY_FLOOR

    def _map_select_cols(self) -> list:
        return ["bk_hash", *self.candidate_keys, self.identity_col]

    def _build_new_assignments(self, incoming: DataFrame) -> DataFrame:
        incoming_keys = (
            incoming.select(*self.candidate_keys)
            .dropna(how="all", subset=self.candidate_keys)
            .distinct()
        )
        incoming_keys = self.with_bk_hash(incoming_keys)

        if self.map_exists:
            existing = self._read_map().select("bk_hash").distinct()
            new_keys = incoming_keys.join(existing, on="bk_hash", how="left_anti")
        else:
            new_keys = incoming_keys

        if new_keys.rdd.isEmpty():
            return new_keys.limit(0).withColumn(self.identity_col, F.lit(None).cast("long"))

        max_id = self._max_identity()
        if len(self.candidate_keys) == 1:
            order_col = F.col(self.candidate_keys[0])
        else:
            order_col = F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in self.candidate_keys],
            )
        w = Window.orderBy(order_col)
        return new_keys.withColumn(
            self.identity_col,
            (F.lit(max_id) + F.row_number().over(w)).cast("long"),
        )

    def persist_new_keys(self, incoming: DataFrame) -> None:
        new_rows = self._build_new_assignments(incoming)
        if new_rows.rdd.isEmpty():
            return

        payload = (
            new_rows.select(*self._map_select_cols())
            .withColumn("dl_map_insert_ts", F.lit(self.batch_time).cast("timestamp"))
            .withColumn("dl_elt_id", F.lit(self.elt_id))
            .withColumn("dl_run_id", F.lit(self.run_id))
        )

        if not self.map_exists:
            self.lakehouse.write_delta_table(
                payload,
                self.map_schema,
                self.map_table,
                None,
                "overwrite",
            )
            self.map_exists = True
            self.map_path = self.lakehouse.build_delta_file_path(self.map_table, self.map_schema)
            return

        delta_map = DeltaTable.forPath(spark, self.map_path)
        (
            delta_map.alias("target")
            .merge(payload.alias("source"), "source.bk_hash = target.bk_hash")
            .whenNotMatchedInsertAll()
            .execute()
        )

    def resolve(self, df: DataFrame) -> DataFrame:
        self.persist_new_keys(df)

        df_hashed = self.with_bk_hash(df.drop(self.identity_col))
        mapping = self._read_map().select("bk_hash", F.col(self.identity_col))

        resolved = (
            df_hashed.alias("s")
            .join(mapping.alias("m"), on="bk_hash", how="left")
            .drop(F.col("s.bk_hash"))
        )

        payload_cols = [c for c in df.columns if c != self.identity_col]
        return resolved.select(
            F.col(f"m.{self.identity_col}").alias(self.identity_col),
            *[F.col(f"s.{c}") for c in payload_cols],
        )


class LoadStrategy(ABC):

    @abstractmethod
    def transform(self, source_data: DataFrame) -> None:
        raise NotImplementedError
    
    def configure_load(
        self,
        target_lakehouse: LakehouseManager,
        target_schema: str,
        target_table: str,
        elt_id: str,
        run_id: str,
        unknown_record: Optional[bool] = None,
        identity: Optional[bool] = None,
        identity_column_name: Optional[str] = None,
        candidate_keys: Optional[List] = None,
        partition_keys: Optional[List] = None,
        batch_time: Optional[datetime] = None,
    ) -> None:
        """
        Configure columns for transform
        """
        if not batch_time:
            batch_time = datetime.now(timezone.utc).isoformat()
        self.batch_time = batch_time

        self._target_schema = target_schema
        self._target_table = target_table
        self._unknown_record = unknown_record
        self._identity = identity
        self._identity_column_name = identity_column_name
        self._candidate_keys = candidate_keys
        self._partition_keys = partition_keys
        self._elt_id = elt_id
        self._run_id = run_id

        self._target_lakehouse = target_lakehouse
        self._target_exists = self._target_lakehouse.check_if_table_exists(self._target_table, self._target_schema)
        self._target_path = self._target_lakehouse.build_delta_file_path(self._target_table, self._target_schema)

        assert candidate_keys if identity else True, "Key columns must be defined when identity column is enabled."

        self._identity_map = None
        if self._identity:
            if not self._identity_column_name:
                self._identity_column_name = (
                    f"{self._target_table}_key".replace("dim_", "", 1).replace("fact_", "", 1)
                )
            self._identity_map = IdentityKeyMap(
                lakehouse=self._target_lakehouse,
                schema=self._target_schema,
                dimension_table=self._target_table,
                identity_column_name=self._identity_column_name,
                candidate_keys=self._candidate_keys,
                elt_id=self._elt_id,
                run_id=self._run_id,
                batch_time=self.batch_time,
            )

    def get_table_metrics(self, table_path: str) -> tuple:
        df = DeltaTable.forPath(spark, table_path).history().where(f"timestamp > '{self.batch_time}'")
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

        return inserts, updates, deletes, output_bytes
    
    @staticmethod
    def add_type_two_metadata(df: DataFrame):
        df = (
                df
                .withColumn("dl_is_current_flag", lit(1).cast("boolean"))
                .withColumn("dl_row_effective_date", expr("to_timestamp('1900-01-01','yyyy-MM-dd')").cast("timestamp"))
                .withColumn("dl_row_expiration_date", expr("to_timestamp('9999-12-31','yyyy-MM-dd')").cast("timestamp"))
            )
        return df

    @staticmethod
    def create_merge_predicate(merge_predicate: list[str]) -> str:
        return " and ".join([f"source.{item} = target.{item}" for item in merge_predicate])
    
    def add_unknown(self, df: DataFrame, type_two_keys: list[str] = None) -> DataFrame:
        unknown_df = spark.createDataFrame(data=spark.sparkContext.emptyRDD(), schema=StructType([]))
        for record_type in [-1,-2]:
            key_desc = "unknown" if record_type == -1 else "not applicable"
            col_list = []

            for col in df.dtypes:
                if not col[0].startswith("dl_"):
                    if col[1] == "string":
                        col_list.append(f"'{key_desc}' as {col[0]}")
                    elif col[1] in ["int", "bigint"]:
                        col_list.append(f"{record_type} as {col[0]}")
                    else:
                        col_list.append(f"null as {col[0]}")

            unknown_df = unknown_df.unionByName(spark.sql("select " + ",".join(col for col in col_list)), allowMissingColumns=True)
            
        metadata = Metadata(
            {
                "meta_column_prefix": "dl_",
                "batch_time": self.batch_time,
                "elt_id": self._elt_id,
                "run_id": self._run_id,
            },
            sorted(unknown_df.columns),
        )
        unknown_df = metadata.add_metadata_columns(unknown_df)
        
        if type_two_keys:
            hash_key = ",".join(c for c in type_two_keys)
            unknown_df = self.add_type_two_metadata(df=unknown_df)
            unknown_df = (
                unknown_df
                .withColumn("merge_key", expr(f"sha2(concat_ws('||', {hash_key}), 256)"))
                .withColumn("dl_row_hash", expr(f"sha2(concat_ws('||', {hash_key}), 256)"))
            )

        df = df.unionByName(unknown_df, allowMissingColumns=True)
            
        return df
    
    def add_identity(self, df: DataFrame) -> DataFrame:
        """Resolve durable bigint identity from the hash map.

        Does not remint keys on OverwriteLoad. SCD2 versions of the same
        business key share the same surrogate so facts do not need reload.
        """
        if not self._candidate_keys:
            raise ValueError("candidate_keys must be defined when identity=True")
        if not getattr(self, "_identity_map", None):
            if not self._identity_column_name:
                self._identity_column_name = (
                    f"{self._target_table}_key".replace("dim_", "", 1).replace("fact_", "", 1)
                )
            self._identity_map = IdentityKeyMap(
                lakehouse=self._target_lakehouse,
                schema=self._target_schema,
                dimension_table=self._target_table,
                identity_column_name=self._identity_column_name,
                candidate_keys=self._candidate_keys,
                elt_id=self._elt_id,
                run_id=self._run_id,
                batch_time=self.batch_time,
            )
        return self._identity_map.resolve(df)
    
    @staticmethod
    def get_upd_col(df: DataFrame, exclusions: list[str]) -> dict:
        upd_col = {}
        for i in df.columns:
            if(i in exclusions):
                continue
            upd_col[i] = "source." + i

        return upd_col


class TypeTwoLoad(LoadStrategy):
    def transform(self, source_data: DataFrame) -> None:
        df = self.add_type_two_metadata(source_data)
        

        if self._target_exists:
            deltaTableDest = DeltaTable.forPath(spark, self._target_path)

            # union incoming records again if they are different from the target (need to expire and create new records)
            df = (
                df
                .withColumn("merge_key", sha2(concat_ws('||', *(col(c).cast("string") for c in self._candidate_keys)), 256))
                
                .unionAll(
                    df.alias("source")
                    .join(deltaTableDest.toDF().alias("target"), expr(self.create_merge_predicate(self._candidate_keys) + " and target.dl_is_current_flag = True and target.dl_row_hash <> source.dl_row_hash"), how="inner")
                    .selectExpr("source.*")
                    .withColumn("merge_key", lit(None))
                    .withColumn("dl_row_effective_date", lit(self.batch_time).cast("timestamp"))
                )
            )

            if self._identity:
                df = self.add_identity(df)
            if self._unknown_record:
                df = self.add_unknown(df=df, type_two_keys=self._candidate_keys)
            upd_col = self.get_upd_col(df=df, exclusions=["merge_key"])
            hash_predicate = ", ".join(f"target.{item}" for item in self._candidate_keys)

            (
                deltaTableDest
                .alias("target")
                .merge(
                    df.alias("source"),
                    f"source.merge_key = sha2(concat_ws('||', {hash_predicate}), 256)"
                )
                .whenMatchedUpdate(
                    "target.dl_row_hash <> source.dl_row_hash and target.dl_is_current_flag = True",
                    set = {
                        "dl_is_current_flag": "False",
                        "dl_row_expiration_date": f"cast('{self.batch_time}' as timestamp)",
                        "dl_row_update_timestamp": f"cast('{self.batch_time}' as timestamp)"
                    }
                )
                .whenNotMatchedInsert(values=upd_col) # use upd_col because it doesn't contain the merge_key column
                .execute()
            )

        else:
            if self._identity:
                df = self.add_identity(df=df)
            if self._unknown_record:
                df = self.add_unknown(df=df, type_two_keys=self._candidate_keys)
                df = df.drop("merge_key")
            self._target_lakehouse.write_delta_table(df, self._target_schema, self._target_table, self._partition_keys, "overwrite")


class MergeLoad(LoadStrategy):
    def transform(self, source_data: DataFrame) -> None:
        df = source_data
        if self._identity:
            df = self.add_identity(df=df)
            
        if self._unknown_record:
            df = self.add_unknown(df=df)
        
        if self._target_exists:
            deltaTableDest = DeltaTable.forPath(spark, self._target_path)
            
            # Exclude columns that are not meant to be updated in the merge
            upd_col = self.get_upd_col(df=df, exclusions=["dl_row_insert_timestamp"])

            (
                deltaTableDest.alias("target")
                .merge(
                    df.alias("source"),
                    self.create_merge_predicate(self._candidate_keys)
                )
                .whenMatchedUpdate("target.dl_row_hash <> source.dl_row_hash", set=upd_col)
                .whenNotMatchedInsertAll()
                .execute()
            )
        
        else:
            self._target_lakehouse.write_delta_table(df, self._target_schema, self._target_table, self._partition_keys, "overwrite")


class OverwriteLoad(LoadStrategy):
    def transform(self, source_data: DataFrame) -> None:
        if self._identity:
            source_data = self.add_identity(df=source_data)
        self._target_lakehouse.write_delta_table(
            source_data,
            self._target_schema,
            self._target_table,
            self._partition_keys,
            "overwrite",
            mergeSchema=True
        ) 


class AppendLoad(LoadStrategy):
    def transform(self, source_data: DataFrame) -> None:
        self._target_lakehouse.write_delta_table(source_data, self._target_schema, self._target_table, self._partition_keys, "append", mergeSchema=True)


class OverwriteByKey(LoadStrategy):
    def transform(self, source_data: DataFrame) -> None:
        if self._identity:
            source_data = self.add_identity(df=source_data)

        if self._target_exists:
            deltaTableDest = DeltaTable.forPath(spark, self._target_path)
            delete_df = source_data.select(*self._candidate_keys).distinct()
            (
                deltaTableDest.alias("target")
                .merge(
                    delete_df.alias("source"),
                    self.create_merge_predicate(self._candidate_keys)
                )
                .whenMatchedDelete()
                .execute()
            )

        self._target_lakehouse.write_delta_table(source_data, self._target_schema, self._target_table, self._partition_keys, "append", mergeSchema=True)


class LoadTypes:
    class LoadStrategies:
        TYPE_TWO_LOAD = TypeTwoLoad
        MERGE_LOAD = MergeLoad
        OVERWRITE_LOAD = OverwriteLoad
        APPEND_LOAD = AppendLoad
        OVERWRITE_BY_KEY_LOAD = OverwriteByKey

    @classmethod
    def get_load_type(cls, load_type: str):
        load_type = load_type.upper()
        if not load_type.endswith("_LOAD"):
            load_type += "_LOAD"

        load_strategy = getattr(cls.LoadStrategies, load_type)

        return load_strategy()
