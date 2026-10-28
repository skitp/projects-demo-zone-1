import great_expectations as gx
import json
import warnings
import threading
import re
import logging
from datetime import datetime, timezone
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    lit,
    expr,
    array,
    col,
    sha2,
    concat_ws,
    date_format,
    struct,
    when,
    explode,
    to_timestamp,
    concat,
    array_contains,
    coalesce,
    trim,
    lower,
)
from typing import Optional
from jsonschema import validate

from spark_engine.data_quality.quarantine import QuarantineTypes
from spark_engine.common.lakehouse import LakehouseManager
from spark_engine.sparkconf import spark

warnings.filterwarnings(
    "ignore", category=DeprecationWarning, module="great_expectations"
)

logger = logging.getLogger(__name__)


class ExpectColumnValuesToBeInLookup(gx.expectations.ExpectColumnValuesToNotBeNull):
    """
    Create a new GX class to use the name and the expectation suite will be updated for this any of these expectations.
    """

    pass

class ExpectKeyToNotExist(gx.expectations.ExpectColumnValuesToBeNull):
    """
    Create a new GX class to use the name and the expectation suite will be updated for this any of these expectations.
    """
    column: str = "placeholder"


class Expectations:
    DQ_LAKEHOUSE = "den_lhw_pdi_001_observability"
    DQ_SCHEMA = "data_quality"
    DQ_RULE_MASTER = "dim_dq_rule_master"
    DQ_ERROR_EVENT = "fact_dq_error_event"
    DQ_ERROR_EVENT_DETAIL = "fact_dq_error_event_detail"

    # Class-level lock - shared by all instances
    _gx_lock = threading.Lock()

    def __init__(
        self,
        lakehouse_name: str,
        schema_name: str,
        table_name: str,
        batch_data: DataFrame,
        expectations: Optional[dict] = None,
    ):
        self.expectations = expectations
        self.lakehouse_name = lakehouse_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.batch_data = batch_data
        self.lakehouse = LakehouseManager(lakehouse_name)
        self.table_path = self.lakehouse.build_delta_file_path(table_name, schema_name)
        self.dq_lakehouse = LakehouseManager(self.DQ_LAKEHOUSE)

    def set_expectations(
        self,
        dq_object: str,
        dq_schema: str,
        dq_lakehouse: str,
    ):
        """
        Set the expectations from the dq rule master table.
        """
        dq_rule_master_path = self.dq_lakehouse.build_delta_file_path(
            self.DQ_RULE_MASTER, self.DQ_SCHEMA
        )

        if not self.dq_lakehouse.check_if_table_exists(
            self.DQ_RULE_MASTER, self.DQ_SCHEMA
        ):
            self.expectations = []
            return self

        dq_rule_constraint_schema = {
            "type": "object",
            "properties": {
                "type": {"type": "string"},
                "kwargs": {"type": "object"},
                "meta": {"type": "object"},
            },
            "required": ["type", "kwargs"],
        }

        config_df = (
            spark.read.load(dq_rule_master_path)
            .where("is_current_flag = true")
            .where(
                f"'{datetime.now(timezone.utc)}' between row_effective_date and row_expiration_date"
            )
            .where(f"dq_rule_applicable_object = '{dq_object}'")
            .where(f"dq_rule_applicable_schema = '{dq_schema}'")
            .where(f"dq_rule_applicable_lakehouse = '{dq_lakehouse}'")
            .select("dq_rule_master_key", "dq_rule_id", "dq_rule_constraint")
        )

        config = []

        for row in config_df.collect():
            dq_rule_constraint = json.loads(row["dq_rule_constraint"])
            validate(instance=dq_rule_constraint, schema=dq_rule_constraint_schema)

            dq_rule_constraint.update(
                {
                    "meta": {
                        **dq_rule_constraint.get("meta", {}),
                        "dq_rule_master_key": row["dq_rule_master_key"],
                        "dq_rule_id": row["dq_rule_id"],
                    }
                }
            )
            config.append(dq_rule_constraint)

        self.expectations = config
        return self

    @staticmethod
    def _extract_sql_condition(unexpected_index_query: str) -> str:
        """
        Robustly extract the SQL boolean condition from Great Expectations unexpected_index_query.
        
        Handles both pre-1.24.0 formats (unquoted) and 1.24.0+ formats where the condition
        is rendered as a quoted SQL string inside a valid Python expression:
          df.filter(F.expr("NOT (`col` IS NOT NULL)"))
        
        Returns a clean SQL expression suitable for Spark's expr() / when().
        This prevents the DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE that occurred when
        a quoted string was passed to when() as a condition after GE 1.24.0.
        """
        if not unexpected_index_query or not isinstance(unexpected_index_query, str):
            return ""

        query = unexpected_index_query.strip()

        # Strip common wrappers: df.filter(...), F.expr(...), etc.
        for prefix in ("df.filter(", "dataframe.filter(", "F.expr(", "expr("):
            if query.lower().startswith(prefix.lower()):
                query = query[len(prefix):]
                break

        # Strip trailing closing parentheses
        query = query.rstrip(")").strip()

        # Unwrap quoted string (GE 1.24.0+ renders the condition as a quoted SQL string)
        if (len(query) >= 2 and query[0] == query[-1] and query[0] in ('"', "'")):
            inner = query[1:-1]
            # Unescape
            inner = inner.replace('\\"', '"').replace("\\'", "'").replace("\\\\", "\\")
            query = inner

        # Clean residual GE artifacts
        query = query.replace("unspecifiedframe$()", "")
        query = re.sub(r"\s+", " ", query).strip()

        return query

    @staticmethod
    def _fix_gx_filter(filter_string: str) -> str:
        """
        Normalize Great Expectations filter strings so they are valid Spark SQL boolean expressions.
        Improved for GE 1.24.0+ (quoted conditions) and legacy formats.
        """
        if not filter_string:
            return filter_string

        value = filter_string

        # Normalize IN lists (ensure string values are quoted)
        if re.search(r"\bIN\s*\(", value, re.IGNORECASE):
            try:
                m = re.search(r"IN\s*\(([^)]+)\)", value, re.IGNORECASE)
                if m:
                    raw_list = m.group(1)
                    items = [i.strip().strip("'\"") for i in raw_list.split(",")]
                    quoted_items = []
                    for item in items:
                        # Keep numeric values unquoted; quote everything else
                        if item.replace(".", "", 1).replace("-", "", 1).isdigit():
                            quoted_items.append(item)
                        else:
                            quoted_items.append(f"'{item}'")
                    quoted = ", ".join(quoted_items)
                    value = value[: m.start(1)] + quoted + value[m.end(1) :]
            except Exception as ex:
                logger.debug("Could not normalize IN list: %s (%s)", filter_string, ex)

        # Normalize RLIKE / REGEXP patterns
        if re.search(r"\b(RLIKE|REGEXP)\b", value, re.IGNORECASE):
            value = value.replace("\\\\", "\\")
            # Ensure unquoted patterns become quoted
            value = re.sub(
                r"(RLIKE|REGEXP)\s+([^'\"\s][^\s]*)",
                r"\1 '\2'",
                value,
                flags=re.IGNORECASE,
            )

        # Remove residual window artifacts from uniqueness expectations
        value = value.replace("unspecifiedframe$()", "")

        return value.strip()

    def _validate_value_sets(self):
        for e in self._expectation_suite.expectation_configurations:
            if e.meta.get("action", {}).get("quarantine"):
                for v in e.kwargs.get("value_set", []):
                    assert (
                        v.find(",") == -1
                    ), f"Value set '{v}' contains a comma which is not supported for expectation quarantine."

    def _get_gx_validator(self):
        context = gx.get_context()
        asset = context.data_sources.add_spark("spark").add_dataframe_asset(
            "data-quality"
        )
        return context.get_validator(
            batch_request=asset.build_batch_request({"dataframe": self.batch_data})
        )

    def quarantine(self, quarantine_strategy:str = None):
        """
        Quarantine failed DQ records
        """
        df = self.expectation_df.where("dq_quarantine_flag = true").drop(
            "dq_quarantine_flag"
        )
        quarantine_type = "DATA_QUALITY"
        quarantine = QuarantineTypes.get_quarantine_type(quarantine_type)
        quarantine.configure(
            self.table_path,
            self.index_column_names,
            quarantine_strategy
        )
        quarantine.quarantine(df)
        self.quarantine_metrics = quarantine.quarantine_metrics

        self.expectation_df = self.expectation_df.where(
            "dq_quarantine_flag = false"
        ).drop("dq_quarantine_flag")

        return self

    def perform_expectations(
        self,
        index_column_names: list[str],
        catch_exceptions: Optional[bool] = False,
        expectation_suite_name: Optional[str] = "not-defined",
    ) -> dict:

        execution_start_time = str(datetime.now(timezone.utc))
        result_format = {
            "result_format": "COMPLETE",
            "exclude_unexpected_values": True,
            "partial_unexpected_count": False,
        }
        self.index_column_names = index_column_names
        # if index_column_names:
        #     result_format["unexpected_index_column_names"] = index_column_names

        assert self.index_column_names, "Key columns are required for data quality tests. Ensure a valid list of columns is provided."

        self._expectation_suite = gx.core.ExpectationSuite(
            name=expectation_suite_name, expectations=self.expectations
        )
        self._dq_drop_cols = []
        self._validate_value_sets()
        self._set_lookup_config()
        self._set_key_not_exists_config()
        self._set_mostly()

        # Only the GX part is locked (minimal contention)
        with Expectations._gx_lock:
            validator = self._get_gx_validator()
            result = validator.validate(
                expectation_suite=self._expectation_suite,
                result_format=result_format,
                catch_exceptions=catch_exceptions,
            ).to_json_dict()

        overall_fail = False
        overall_warn = False

        self.expectation_df = (
            self.batch_data
            .withColumn(
                "dq_index", 
                array(*[col(c).cast("string") for c in self.index_column_names]).cast("array<string>")
            )
            .withColumn("dq_index_delimited", concat_ws(",", *self.index_column_names))
        )

        expectation_result = []
        dq_meta_col = []
        dq_meta_col_count = 1

        for r in result["results"]:
            fail_threshold = (
                r["expectation_config"]
                .get("meta", {})
                .get("action", {})
                .get("fail_threshold", float(100.0))
            )
            warn_threshold = (
                r["expectation_config"]
                .get("meta", {})
                .get("action", {})
                .get("warn_threshold", float(0.0))
            )
            quarantine_flag = (
                r["expectation_config"]
                .get("meta", {})
                .get("action", {})
                .get("quarantine", False)
            )

            unexpected_percent = r["result"].get("unexpected_percent", 0)
            if not unexpected_percent:
                unexpected_percent = 0

            # validate meta actions
            assert type(fail_threshold) in [
                int,
                float,
            ], f"fail_threshold must be [int, float]. Type provided is {type(fail_threshold)}"
            assert type(warn_threshold) in [
                int,
                float,
            ], f"warn_threshold must be [int, float]. Type provided is {type(warn_threshold)}"
            assert type(quarantine_flag) in [
                bool
            ], f"quarantine_flag must be 'bool'. Type provided is {type(quarantine_flag)}"

            fail = True if (100 - unexpected_percent) < fail_threshold else False
            warn = True if (100 - unexpected_percent) < warn_threshold else False
            unexpected_index_query = r["result"].get("unexpected_index_query")
            dq_rule_id = r["expectation_config"]["meta"]["dq_rule_id"]
            dq_rule_master_key = r["expectation_config"]["meta"]["dq_rule_master_key"]

            r["expectation_config"]["kwargs"].pop("batch_id", None)
            expectation_result.append(
                {
                    "success": r["success"],
                    "expectation_config": r["expectation_config"],
                    "result": r["result"],
                    "exception_info": r["exception_info"],
                    "fail": fail,
                    "warn": warn,
                    "quarantine": quarantine_flag,
                }
            )

            if fail:
                overall_fail = True
            if warn:
                overall_warn = True

            dq_quarantine_flag = False
            if result["success"] and quarantine_flag and unexpected_index_query:
                dq_quarantine_flag = True
            elif quarantine_flag and not unexpected_index_query:
                raise ValueError(
                    f"Quarantine enabled for rule: '{dq_rule_id}' but this great_expectations expectation does not return necessary unexpected values."
                )

            if unexpected_index_query:
                # GE 1.24.0+ returns a valid Python expression whose condition is a quoted SQL string.
                # Extract a clean boolean SQL expression so Spark's when() receives a boolean
                # (avoids DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE).
                raw_condition = self._extract_sql_condition(unexpected_index_query)
                expectation_filter = self._fix_gx_filter(raw_condition)

                if not expectation_filter:
                    logger.warning(
                        "Empty or unusable filter extracted from unexpected_index_query for rule %s: %s",
                        dq_rule_id,
                        unexpected_index_query,
                    )
                else:
                    logger.debug(
                        "DQ filter for rule %s (raw=%s): %s",
                        dq_rule_id,
                        unexpected_index_query[:120],
                        expectation_filter,
                    )

                    dq_meta_col.append(
                        {
                            "column_name": f"dq_meta_col_{dq_meta_col_count}",
                            "column_value": lit(
                                when(
                                    expr(expectation_filter),
                                    array(
                                        struct(
                                            lit(dq_rule_master_key).cast("int").alias("dq_rule_master_key"),
                                            lit(dq_rule_id).cast("string").alias("dq_rule_id"),
                                            lit(unexpected_percent).cast("double").alias(
                                                "dq_unexpected_percent"
                                            ),
                                            lit(dq_quarantine_flag).cast("boolean").alias("dq_quarantine_flag"),
                                        )
                                    ),
                                )
                            ),
                        }
                    )
                    dq_meta_col_count += 1

        if dq_meta_col == []:
            dq_meta_col.append(
                {
                    "column_name": "dq_meta_col_1",
                    "column_value": lit(None).cast(
                        "array<struct<dq_rule_master_key:int,dq_rule_id:string,dq_unexpected_percent:double,dq_quarantine_flag:boolean>>"
                    )
                }
            )

        self.expectation_df = (
            self.expectation_df.withColumns(
                {
                    column["column_name"]: column["column_value"]
                    for column in dq_meta_col
                }
            )
            .withColumn(
                "dq_meta",
                concat(
                    *[coalesce(col(column["column_name"]), lit(array())) for column in dq_meta_col]
                )
            )
            .withColumn(
                "dq_quarantine_flag",
                when(
                    array_contains("dq_meta.dq_quarantine_flag", True),
                    lit(True)
                ).otherwise(lit(False))
            )
            .drop(*[col(column["column_name"]) for column in dq_meta_col])
            .withColumn(
                "dq_success",
                expr(
                    "case when cast(dq_meta as array<string>) = array() then true else false end"
                ),
            )
        )

        self.expectation_metrics = {
            "success": result["success"],
            "fail": overall_fail,
            "warn": overall_warn,
            "lakehouse": self.lakehouse_name,
            "schema": self.schema_name,
            "table": self.table_name,
            "execution_start_time": execution_start_time,
            "execution_end_time": str(datetime.now(timezone.utc)),
            "statistics": result["statistics"],
            "results": expectation_result,
        }

        self.expectation_df = self.expectation_df.drop(*self._dq_drop_cols)
        if self.expectation_df.where("dq_success = false").count():
            self._log_expectation_results(execution_start_time)

        return self

    def _log_expectation_results(self, execution_start_time: datetime) -> None:
        assert self.dq_lakehouse.check_if_table_exists(
            self.DQ_ERROR_EVENT, self.DQ_SCHEMA
        ), f"Data quality event table '{self.DQ_ERROR_EVENT}' is missing."
        assert self.dq_lakehouse.check_if_table_exists(
            self.DQ_ERROR_EVENT_DETAIL, self.DQ_SCHEMA
        ), f"Data quality event detail table '{self.DQ_ERROR_EVENT_DETAIL}' is missing."

        dq_df = (
            self.expectation_df.withColumn("dl_eltid", lit(None).cast("string"))
            if "dl_eltid" not in self.expectation_df.columns
            else self.expectation_df
        )

        error_event_df = (
            dq_df.where("dq_success = false")
            .select(explode("dq_meta").alias("dq_meta"), "dl_eltid")
            .select(
                sha2(
                    concat_ws(
                        "||", "dq_meta.dq_rule_master_key", lit(execution_start_time)
                    ),
                    256,
                ).alias("dq_error_event_key"),
                date_format(lit(execution_start_time), "yyyyMMdd")
                .cast("int")
                .alias("dq_error_event_date_key"),
                col("dq_meta.dq_rule_master_key").alias("dq_rule_master_key"),
                col("dl_eltid").alias("dp_processing_batch_key"),
                date_format(lit(execution_start_time), "HH:mm:ss.SSSSSS").alias(
                    "time_of_day"
                ),
                col("dq_meta.dq_unexpected_percent").cast("double").alias("severity_score"),
                to_timestamp(lit(execution_start_time)).alias(
                    "error_event_timestamp"
                ),
            )
            .distinct()
        )
        self.dq_lakehouse.write_delta_table(
            error_event_df, self.DQ_SCHEMA, self.DQ_ERROR_EVENT, None, "append"
        )

        error_event_detail_df = (
            dq_df.where("dq_success = false")
            .select(explode("dq_meta").alias("dq_meta"), "dq_index", "dq_index_delimited")
            .select(
                sha2(
                    concat_ws(
                        "||",
                        "dq_meta.dq_rule_master_key",
                        "dq_index",
                        lit(execution_start_time),
                    ),
                    256,
                ).alias("dq_error_event_detail_key"),
                sha2(
                    concat_ws(
                        "||", "dq_meta.dq_rule_master_key", lit(execution_start_time)
                    ),
                    256,
                ).alias("dq_error_event_key"),
                col("dq_meta.dq_rule_master_key").alias("dq_rule_master_key"),
                date_format(lit(execution_start_time), "HH:mm:ss.SSSSSS").alias(
                    "time_of_day"
                ),
                col("dq_index").alias("tested_record_identifier"),
                col("dq_index_delimited").alias("tested_record_identifier_delimited"),
                to_timestamp(lit(execution_start_time)).alias(
                    "error_event_timestamp"
                ),
            )
        )
        self.dq_lakehouse.write_delta_table(
            error_event_detail_df,
            self.DQ_SCHEMA,
            self.DQ_ERROR_EVENT_DETAIL,
            None,
            "append",
        )

    def _set_lookup_config(self):
        pre_join_count = 0
        for i, e in enumerate(self._expectation_suite.expectations):
            if type(e) == ExpectColumnValuesToBeInLookup:
                column = e.column
                lookup_config = e.meta.get("lookup_config", {})
                lookup_lakehouse = lookup_config.get("lakehouse")
                lookup_schema = lookup_config.get("schema")
                lookup_table = lookup_config.get("table")
                lookup_column = lookup_config.get("column")
                lookup_filter = lookup_config.get("filter")
                allow_missing_lookup = lookup_config.get("allow_missing_lookup", False)
                allow_null_values = lookup_config.get("allow_null_values", False)
                source_date_column = lookup_config.get("date_column")
                lookup_start_date = lookup_config.get("start_date_column")
                lookup_end_date = lookup_config.get("end_date_column")
                dq_column_name = f"dq_{column}_{i}"

                assert (
                    lookup_lakehouse
                ), "Lookup lakehouse is missing for expect_column_values_to_be_in_lookup expectation. It should exist in the expectation meta.lookup_config.lakehouse."
                assert (
                    lookup_schema
                ), "Lookup schema is missing for expect_column_values_to_be_in_lookup expectation. It should exist in the expectation meta.lookup_config.schema."
                assert (
                    lookup_table
                ), "Lookup table is missing for expect_column_values_to_be_in_lookup expectation. It should exist in the expectation meta.lookup_config.table."
                assert (
                    lookup_column
                ), "Lookup column is missing for expect_column_values_to_be_in_lookup expectation. It should exist in the expectation meta.lookup_config.column."
                assert isinstance(allow_missing_lookup, bool), f"allow_missing_lookup expected a bool value but got {type(allow_missing_lookup)}."

                # if one date column is specified then all must exist, otherwise none of them are expected
                date_columns = [
                    source_date_column,
                    lookup_start_date,
                    lookup_end_date
                ]
                assert (
                    all(c is None for c in date_columns) or all(date_columns)
                ), "All date columns (date_column, start_date_column, end_date_column) must be specified when using a lookup start and end date."

                date_filter = True if all(date_columns) else False
                lakehouse = LakehouseManager(lookup_lakehouse)

                if allow_missing_lookup and not lakehouse.check_if_table_exists(lookup_table, lookup_schema):
                    self.batch_data = self.batch_data.withColumn(dq_column_name, lit("1"))
                else:
                    lookup_path = lakehouse.build_delta_file_path(lookup_table, lookup_schema)
                    
                    lookup_df = spark.read.load(lookup_path)
                    if lookup_filter:
                        lookup_df = lookup_df.filter(lookup_filter)

                    select_columns = [
                        col(lookup_column).alias(dq_column_name)
                    ]

                    join_condition = [
                        trim(lower(col(f"d.{column}"))) == trim(lower(col(f"l{i}.{dq_column_name}"))),
                    ]

                    if date_filter:
                        if not pre_join_count:
                            pre_join_count = self.batch_data.count()
                        join_condition.extend(
                            [
                                col(f"d.{source_date_column}") >= col(f"l{i}.{lookup_start_date}"),
                                col(f"d.{source_date_column}") <= col(f"l{i}.{lookup_end_date}"),
                            ]
                        )

                        select_columns.extend(
                            [
                                col(lookup_start_date),
                                col(lookup_end_date)
                            ]
                        )

                    lookup_df = (lookup_df
                        .select(select_columns)
                        .distinct()
                    )
                    
                    self.batch_data = (
                        self.batch_data.alias("d")
                        .join(
                            lookup_df.alias(f"l{i}"),
                            join_condition,
                            "left",
                        )
                        .select(
                            "d.*",
                            f"l{i}.{dq_column_name}"
                        )
                    )

                    if date_filter:
                        if pre_join_count != self.batch_data.count():
                            raise ValueError(
                                "The lookup using date columns generated extra rows."\
                                "Check that there are no overlapping dates in the lookup table."\
                                f"Lookup config: {lookup_config}"
                            )
                    

                    if allow_null_values:
                        for c in self.batch_data.dtypes:
                            if c[0] == dq_column_name:
                                data_type = c[1]
                                match data_type:
                                    case "date" | "timestamp":
                                        null_value = "1900-1-1"
                                    case _:
                                        null_value = "0"

                        self.batch_data = (
                            self.batch_data
                            .withColumn(
                                dq_column_name,
                                when(col(column).isNull(), lit(null_value).cast(data_type))
                                .otherwise(col(dq_column_name))
                            )
                        )

                self._dq_drop_cols.append(dq_column_name)
                e.column = dq_column_name

    def _set_key_not_exists_config(self):
        for i, e in enumerate(self._expectation_suite.expectations):
            if type(e) == ExpectKeyToNotExist:
                expectation_config = e.meta.get("config", {})
                target_filter = expectation_config.get("filter")
                hash_check = expectation_config.get("hash_check", False)
                assert isinstance(hash_check, bool), f"expect_key_to_not_exist hash_check expected a bool value but got {type(hash_check)}."
                dq_column_name = "dq_key_not_exists_column"
                
                if self.lakehouse.check_if_table_exists(self.table_name, self.schema_name):
                    target_df = spark.read.load(self.table_path)
                    if target_filter:
                        target_df = target_df.filter(target_filter)

                    join_condition = [
                        trim(lower(col(f"d.{c}"))) == trim(lower(col(f"l.{c}")))
                        for c in self.index_column_names
                    ]
                      
                    if hash_check:
                        row_hash_column = "dl_rowhash" if "dl_rowhash" in self.batch_data.columns else "dl_row_hash"
                        sort_column = "dl_lastmodifiedutc" if "dl_lastmodifiedutc" in self.batch_data.columns else "dl_row_update_timestamp"
                        join_condition.append(
                            col(f"d.{row_hash_column}") != col(f"l.{row_hash_column}")
                        )

                        target_df = (target_df
                            .select(
                                *self.index_column_names,
                                row_hash_column,
                                sort_column
                            )
                            .orderBy(col(sort_column).desc())
                            .dropDuplicates(self.index_column_names)
                            .drop(sort_column)
                        )

                    else:
                        target_df = (target_df
                            .select(
                                *self.index_column_names
                            )
                            .distinct()
                        )

                    self.batch_data = (
                        self.batch_data.alias("d")
                        .join(
                            target_df.alias("l"),
                            join_condition,
                            "left",
                        )
                        .selectExpr(
                            "d.*",
                            f"l.{self.index_column_names[0]} as {dq_column_name}"
                        )
                    )
                else:
                    self.batch_data = self.batch_data.selectExpr(
                        "*",
                        f"null as {dq_column_name}"
                    )

                self._dq_drop_cols.append(dq_column_name)
                e.column = dq_column_name

    def _set_mostly(self):
        for e in self._expectation_suite.expectations:
            fail_threshold = e.meta.get("action", {}).get(
                "fail_threshold", float(100.0)
            )
            assert type(fail_threshold) in [
                int,
                float,
            ], f"fail_threshold must be [int, float]. Type provided is {type(fail_threshold)}"
            if "mostly" in e.get_allowed_config_keys():
                e.mostly = fail_threshold / 100
