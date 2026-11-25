import json
import yaml
import threading
import fsspec
import logging
import re
from datetime import datetime, timezone
from typing import List, Optional, Dict
from pyspark.sql.functions import lit, col
from pyspark.sql.dataframe import DataFrame
from pyspark.storagelevel import StorageLevel

from spark_engine.common.lakehouse import LakehouseManager
from spark_engine.sparkconf import spark
from spark_engine.transform.strategy import LoadTypes
from spark_engine.transform.metrics import TransformMetrics, GroupMetrics
from spark_engine.transform.metadata import Metadata
from spark_engine.data_quality.quarantine import QuarantineVersion, QuarantineLog
from spark_engine.data_quality.data_quality import Expectations
import spark_engine.common.constants as constants

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

_GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


class Transform:
    """
    The transform class which takes the metadata config and processes a single table to the lakehouse.

    Cross-workspace support (simple model):
    - Call set_store_workspace_id(store_workspace_id) from the notebook before start_transform.
    - All source and target lakehouses named in the YAML are assumed to live in that same workspace.
    - Observability lakehouse always resolves in the workspace where the code is executing.
    - When store_workspace_id is set, lakehouse names are resolved to IDs via the Fabric API
      (notebookutils.lakehouse.get only sees the current workspace) and then passed to LakehouseManager.
    - No changes to YAML files are required.
    """
    # Class-level lock - shared by all instances
    _yaml_lock = threading.Lock()
    # Cache of workspace_id -> {lakehouse_name_lower: lakehouse_id}
    _lakehouse_id_cache: Dict[str, Dict[str, str]] = {}
    _cache_lock = threading.Lock()

    def __init__(
        self,
        config_path: str,
        filesystem_code: Optional[str] = "abfss"
    ) -> None:
        self.batch_time = datetime.now(timezone.utc).isoformat()
        self.transform_config_path = config_path
        self.raise_errors = True
        self.store_workspace_id: Optional[str] = None

        storage_options = {
            "account_name": "onelake",
            "account_host": "onelake.dfs.fabric.microsoft.com",
        }
        onelake_fs = fsspec.filesystem(filesystem_code, **storage_options)

        with Transform._yaml_lock:  # <- only this block is serialized
            with onelake_fs.open(self.transform_config_path, "r") as yaml_file:
                self.transform_config = yaml.safe_load(yaml_file)

        self.target_table = self.transform_config["target"]["table"]
        self.target_schema = self.transform_config["target"]["schema"]
        self.target_lakehouse_name = self.transform_config["target"]["lakehouse"]
        self.load_strategy = self.transform_config["target"]["load_strategy"]
        self.key_columns = self.transform_config["target"].get("key_columns")
        self.unknown_record = self.transform_config["target"].get("unknown_record")
        self.identity = self.transform_config["target"].get("identity")
        self.identity_column_name = self.transform_config["target"].get("identity_column_name")
        self.partition_keys = self.transform_config["target"].get("partition_by")
        self.pii_columns = self.transform_config["target"].get("pii_columns")
        self.pii_table_name = self.transform_config["target"].get("pii_table_name")
        self.quarantine_strategy = self.transform_config["target"].get("quarantine_strategy")
        self.sources = self.transform_config.get("source", [])
        self.queries = self.transform_config.get("query", [])

        # Defer LakehouseManager creation until after set_store_workspace_id
        # (or until start_transform for same-workspace runs). Creating it here
        # would call notebookutils.lakehouse.get on the target name and fail
        # when the lakehouse lives in another workspace.
        self.target_lakehouse = None
        self.target_path = None
        logger.info(
            "Transform config loaded for %s.%s (lakehouse=%s). "
            "Lakehouse resolution deferred until set_store_workspace_id or start_transform.",
            self.target_schema, self.target_table, self.target_lakehouse_name,
        )

    def _is_guid(self, value: str) -> bool:
        return bool(value and _GUID_RE.match(value))

    def _get_fabric_token(self) -> str:
        """Acquire a token for the Fabric REST API (used only for cross-workspace name resolution)."""
        try:
            import notebookutils
            # Prefer the Fabric API audience; fall back to the Power BI audience if needed.
            for audience in (
                "https://api.fabric.microsoft.com",
                "https://api.fabric.microsoft.com/.default",
                "https://analysis.windows.net/powerbi/api",
            ):
                try:
                    token = notebookutils.credentials.getToken(audience)
                    if token:
                        return token
                except Exception:
                    continue
        except Exception as exc:
            logger.warning("notebookutils.credentials.getToken failed: %s", exc)
        raise RuntimeError(
            "Unable to acquire a Fabric API token to resolve lakehouse names in the store workspace. "
            "Ensure the notebook identity has access to the store workspace."
        )

    def _load_lakehouse_map(self, workspace_id: str) -> Dict[str, str]:
        """Return {displayName_lower: id} for all lakehouses in the given workspace. Cached."""
        with Transform._cache_lock:
            if workspace_id in Transform._lakehouse_id_cache:
                return Transform._lakehouse_id_cache[workspace_id]

        logger.info("Resolving lakehouse IDs for store workspace %s via Fabric API", workspace_id)
        token = self._get_fabric_token()
        headers = {"Authorization": f"Bearer {token}"}
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
        mapping: Dict[str, str] = {}

        try:
            import requests
        except ImportError:
            raise RuntimeError("The 'requests' package is required to resolve cross-workspace lakehouse names.")

        while url:
            resp = requests.get(url, headers=headers, timeout=60)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to list lakehouses in workspace {workspace_id}: "
                    f"HTTP {resp.status_code} - {resp.text[:500]}"
                )
            payload = resp.json()
            for item in payload.get("value", []):
                name = (item.get("displayName") or "").strip()
                lid = item.get("id")
                if name and lid:
                    mapping[name.lower()] = lid
            url = payload.get("continuationUri") or payload.get("@odata.nextLink")

        with Transform._cache_lock:
            Transform._lakehouse_id_cache[workspace_id] = mapping
        logger.info("Cached %d lakehouse(s) for workspace %s", len(mapping), workspace_id)
        return mapping

    def _resolve_lakehouse_id(self, lakehouse_name: str, workspace_id: Optional[str]) -> str:
        """
        Return the lakehouse ID to pass to LakehouseManager.
        - If workspace_id is None, return the name (LakehouseManager will use notebookutils).
        - If the name is already a GUID, return it.
        - Otherwise resolve via Fabric API so LakehouseManager receives a GUID and does not
          call notebookutils.lakehouse.get (which only sees the current workspace).
        """
        if not workspace_id:
            return lakehouse_name
        if self._is_guid(lakehouse_name):
            return lakehouse_name

        mapping = self._load_lakehouse_map(workspace_id)
        lid = mapping.get(lakehouse_name.lower())
        if not lid:
            available = ", ".join(sorted(mapping.keys())[:20]) or "<none>"
            raise ValueError(
                f"Lakehouse '{lakehouse_name}' not found in store workspace {workspace_id}. "
                f"Available (first 20): {available}"
            )
        logger.info(
            "Resolved lakehouse '%s' -> %s in workspace %s",
            lakehouse_name, lid, workspace_id,
        )
        return lid

    def _make_lakehouse_manager(self, lakehouse_name: str, workspace_id: Optional[str] = None) -> LakehouseManager:
        """Create a LakehouseManager, resolving the name to an ID when a store workspace is set."""
        resolved = self._resolve_lakehouse_id(lakehouse_name, workspace_id)
        return LakehouseManager(resolved, workspace_id=workspace_id)

    def _init_target_lakehouse(self) -> None:
        """Create / recreate the target LakehouseManager and target_path."""
        self.target_lakehouse = self._make_lakehouse_manager(
            self.target_lakehouse_name,
            workspace_id=self.store_workspace_id,
        )
        self.target_path = self.target_lakehouse.build_delta_file_path(
            self.target_table, self.target_schema,
        )
        logger.info(
            "Target path for %s.%s: %s",
            self.target_schema, self.target_table, self.target_path,
        )

    def set_store_workspace_id(self, store_workspace_id: str):
        """
        Set the workspace ID that owns every source and the target lakehouse
        referenced in the YAML. Must be called before start_transform when
        those lakehouses are not in the workspace where the notebook is running.

        Observability lakehouse is unaffected and always resolves in the
        executing workspace.

        Args:
            store_workspace_id: Fabric workspace GUID for the YAML sources and target.

        Returns:
            self (for method chaining).
        """
        logger.info("Setting store_workspace_id=%s for YAML sources and target", store_workspace_id)
        self.store_workspace_id = store_workspace_id
        self._init_target_lakehouse()
        return self

    def configure_transform(
        self,
        product_name: str,
        feed_name: str,
        dataset_name: str
    ):
        """
        Configure the transform with additional properties.
        """
        self.product_name = product_name
        self.feed_name = feed_name
        self.dataset_name = dataset_name

        return self

    def _set_load_strategy(self, load_type: str):
        load_type = load_type.upper()
        try:
            self._load_type = LoadTypes.get_load_type(load_type)
        except AttributeError as e:
            raise ValueError(f"Load strategy: {load_type} is not supported.") from e

        return self

    def _set_candidate_keys(self, candidate_keys: List[str]) -> List[str]:
        if not candidate_keys:
            return None

        candidate_key_list = [item.lower() for item in candidate_keys]
        if not all(item in self._transform_df.columns for item in candidate_key_list):
            raise ValueError("There are key columns that are not in the source data.")
        return candidate_key_list
    
    def _incremental_filter(self, df: DataFrame, log_path: str):
        last_date_df = (
            spark.read.format("delta").load(log_path)
            .filter("status = 'Succeeded'")
            .filter(f"product_name = '{self.product_name}'")
            .filter(f"feed_name = '{self.feed_name}'")
            .filter(f"dataset_name = '{self.dataset_name}'")
            .select("start_date_time")
            .orderBy(col("start_date_time").desc())
            .limit(1)
        )

        if last_date_df.count():
            last_date = last_date_df.collect()[0][0]

            if "dl_lastmodifiedutc" in df.columns:
                df = df.filter(f"dl_lastmodifiedutc >= '{last_date}'")
            elif "dl_row_update_timestamp" in df.columns:
                df = df.filter(f"dl_row_update_timestamp >= '{last_date}'")
            else:
                raise ValueError("The column 'dl_lastmodifiedutc' or 'dl_row_update_timestamp' was not found in the source. It is required for incremental sources.")

        return df

    def _generate_source_views(self) -> None:
        # Observability lakehouse always lives in the workspace where the code
        # is executing — do not pass store_workspace_id.
        if len(
            [
                source for source
                in self.sources
                if source.get("incremental") == True
            ]
        ) > 0:
            observability_lakehouse = LakehouseManager(constants.OBS_LAKEHOUSE_NAME)
            obs_path = observability_lakehouse.build_delta_file_path(
                constants.SOURCE_INGEST_LOG_TABLE,
                constants.OBS_LAKEHOUSE_SCHEMA
            )

        # Source lakehouses named in the YAML share the store_workspace_id
        for lakehouse in set(source["lakehouse"] for source in self.sources):
            source_lakehouse = self._make_lakehouse_manager(
                lakehouse,
                workspace_id=self.store_workspace_id,
            )
            logger.info(
                "Source lakehouse '%s' resolved (store_workspace_id=%s)",
                lakehouse, self.store_workspace_id,
            )
            
            for source in [
                source for source
                in self.sources
                if source["lakehouse"] == lakehouse
            ]:
                path = source_lakehouse.build_delta_file_path(
                    source["table"], source["schema"]
                )
                source_df = spark.read.format("delta").load(path)

                if source.get("incremental") == True:
                    source_df = self._incremental_filter(source_df, obs_path)

                source_df.createOrReplaceTempView(source["name"])

    def _execute_queries(self) -> None:
        try:
            for query in self.queries:
                query_df = spark.sql(query["sql"])
                view_name = query["name"]

                if query.get("drop"):
                    query_df = query_df.drop(*query.get("drop"))
                query_df.createOrReplaceTempView(view_name)

                if query.get("cache"):
                    cache_type = query.get("cache_type")
                    if cache_type:
                        storage_level = getattr(StorageLevel, cache_type.upper())
                        spark.catalog.cacheTable(view_name, storage_level)
                    else:
                        spark.catalog.cacheTable(view_name)

        except Exception as e:
            if str(e).__contains__(
                "Spark SQL queries are only possible in the context of a lakehouse. Please attach a lakehouse to proceed"
            ):
                raise Exception(
                    "A Spark SQL query referenced a temp view that does not exist. Only reference named sources or queries in the config."
                )
            else:
                raise Exception(e)

        return query_df
    
    def _uncache_queries(self) -> None:
        for query in self.queries:
            view_name = query["name"]

            if query.get("cache"):
                spark.catalog.uncacheTable(view_name)

    def _redact_pii_clone(self):
        if self.pii_columns:
            df = spark.read.format("delta").load(self.target_path)

            df = df.withColumns(
                {
                    column: (
                        lit(None).cast(dtype)
                        if dtype.startswith("decimal")
                        or dtype
                        in [
                            "tinyint",
                            "smallint",
                            "int",
                            "bigint",
                            "float",
                            "double",
                            "binary",
                            "boolean",
                        ]
                        else (
                            lit("1900-1-1").cast(dtype)
                            if dtype in ["date", "timestamp", "timestamp_ntz"]
                            else lit("[REDACTED]").cast("string")
                        )
                    )
                    for column, dtype in df.dtypes
                    if column in self.pii_columns
                }
            )
            pii_table = self.pii_table_name if self.pii_table_name else self.target_table + "_pii"
            self.target_lakehouse.write_delta_table(
                df,
                self.target_schema,
                pii_table,
                self.partition_keys,
                "overwrite",
                mergeSchema=True,
            )

    def _log_quarantine(self, quarantine_metrics: list[dict]):
        quarantine_log = QuarantineLog()
        for q in quarantine_metrics:
            quarantine_log.add_log_data(
                source_system="ELT",
                dataset_name=self.target_table,
                elt_id=self.elt_id,
                run_id=self.run_id,
                total_rows=q["inserts"],
                quarantine_type=q["quarantine_type"],
                log_datetime=str(datetime.now(timezone.utc).isoformat()),
                primary_key_list=",".join(self.key_columns if self.key_columns else []),
            )
        quarantine_log.write_log()

    def start_transform(self, elt_id: str = "0", run_id: str = "0"):
        self.elt_id = elt_id
        self.run_id = run_id

        logger.info(
            "Starting transform for %s.%s (elt_id=%s, run_id=%s, store_workspace_id=%s)",
            self.target_schema,
            self.target_table,
            elt_id,
            run_id,
            self.store_workspace_id,
        )

        quarantine_table = None

        self.METRICS = TransformMetrics()
        self.ALLMETRICS = GroupMetrics()
        self.METRICS.eltId = self.elt_id
        self.METRICS.runId = self.run_id
        self.METRICS.startTime = self.batch_time
        self.METRICS.lakehouse = self.target_lakehouse_name
        self.METRICS.deltaSchema = self.target_schema
        self.METRICS.deltaObject = self.target_table

        try:
            # Ensure target LakehouseManager exists. If set_store_workspace_id was
            # called it is already created; otherwise create it now (same-workspace).
            if self.target_lakehouse is None:
                logger.info(
                    "Initializing target lakehouse for %s.%s (store_workspace_id=%s)",
                    self.target_schema, self.target_table, self.store_workspace_id,
                )
                self._init_target_lakehouse()

            self._set_load_strategy(self.load_strategy)
            self._generate_source_views()
            self._transform_df = self._execute_queries()
            logger.info(
                "Source views and queries completed for %s.%s (%d columns)",
                self.target_schema, self.target_table, len(self._transform_df.columns),
            )

            metadata = Metadata(
                {
                    "meta_column_prefix": "dl_",
                    "batch_time": self.batch_time,
                    "elt_id": self.elt_id,
                    "run_id": self.run_id,
                },
                sorted(self._transform_df.columns),
            )
            self._transform_df = metadata.add_metadata_columns(self._transform_df)

            self.key_columns = self._set_candidate_keys(self.key_columns)

            quarantine_table = (
                QuarantineVersion(None)
                .configure(self.target_path)
                .set_current_version()
            )

            self._load_type.configure_load(
                target_lakehouse=self.target_lakehouse,
                target_schema=self.target_schema,
                target_table=self.target_table,
                elt_id=self.elt_id,
                run_id=self.run_id,
                unknown_record=self.unknown_record,
                identity=self.identity,
                identity_column_name=self.identity_column_name,
                candidate_keys=self.key_columns,
                partition_keys=self.partition_keys,
                batch_time=self.batch_time,
            )

            expectation_success = True
            # Pass the already-resolved LakehouseManager so Expectations does not
            # call notebookutils.lakehouse.get on the lakehouse name.
            dq = Expectations(
                self.target_lakehouse_name,
                self.target_schema,
                self.target_table,
                self._transform_df,
                workspace_id=self.store_workspace_id,
                lakehouse_manager=self.target_lakehouse,
            ).set_expectations(
                self.target_table,
                self.target_schema,
                self.target_lakehouse_name
            )

            if dq.expectations:
                dq.perform_expectations(self.key_columns)
                dq.quarantine(self.quarantine_strategy)
                self._transform_df = dq.expectation_df.drop(
                    *[c for c in dq.expectation_df.columns if c.startswith("dq_")]
                )
                if dq.quarantine_metrics:
                    self.ALLMETRICS.quarantine.append(dq.quarantine_metrics)
                self.ALLMETRICS.expectations = dq.expectation_metrics
                expectation_success = dq.expectation_metrics["success"]

            if expectation_success:
                self._load_type.transform(self._transform_df)
                self.METRICS.success = True

                inserts, updates, deletes, output_bytes = (
                    self._load_type.get_table_metrics(self.target_path)
                )
                self.METRICS.recordInserts = inserts
                self.METRICS.recordUpdates = updates
                self.METRICS.recordDeletes = deletes
                self.METRICS.outputBytes = output_bytes

                self._redact_pii_clone()

            self.METRICS.endTime = datetime.now(timezone.utc).isoformat()
            self.ALLMETRICS.ingestion = self.METRICS.to_dict()

            if self.ALLMETRICS.quarantine:
                self._log_quarantine(self.ALLMETRICS.quarantine)

            logger.info(
                "Transform completed successfully for %s.%s (inserts=%s, updates=%s, deletes=%s)",
                self.target_schema,
                self.target_table,
                getattr(self.METRICS, "recordInserts", None),
                getattr(self.METRICS, "recordUpdates", None),
                getattr(self.METRICS, "recordDeletes", None),
            )

        except Exception as e:
            self.METRICS.error = str(e)
            self.METRICS.endTime = datetime.now(timezone.utc).isoformat()
            logger.error(
                "Transform failed for %s.%s: %s",
                self.target_schema, self.target_table, e,
                exc_info=True,
            )
            if quarantine_table:
                quarantine_table.restore_current_version()
            if self.raise_errors:
                raise Exception(e)
            
        finally:
            self._uncache_queries()

        return self

    def metrics(self, frmt: str = "json"):
        """
        Get the metrics that were generated during the transform process in json or dict format.
        """
        if frmt == "json":
            return self.ALLMETRICS.to_json()
        if frmt == "dict":
            return self.ALLMETRICS.to_dict()
