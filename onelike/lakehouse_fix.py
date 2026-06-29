import re
import notebookutils
from typing import Optional, Union
from delta.tables import DeltaTable
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from spark_engine.common.string_handlers import strip_string
from spark_engine.sparkconf import spark


class LakehouseManager:
    def __init__(self, lakehouse_name: str, workspace_id: Optional[str] = None):
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_name

    @property
    def workspace_id(self):
        return self._workspace_id

    @workspace_id.setter
    def workspace_id(self, value: Optional[str] = None):
        if value and re.match(r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}", value):
            self._workspace_id = value
        else:
            self._workspace_id = None

    @property
    def lakehouse_id(self):
        return self._lakehouse_id

    @lakehouse_id.setter
    def lakehouse_id(self, value: Optional[str] = None):
        if not value:
            self._lakehouse_id = None
            return

        if re.match(r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}", value):
            if not self._workspace_id:
                raise ValueError(
                    "Workspace Id is missing. It must be set when manually passing the Lakehouse GUID."
                )
            self._lakehouse_id = value
        else:
            # Name was passed → resolve with correct kwarg: workspaceId (camelCase)
            try:
                if self._workspace_id:
                    lakehouse = notebookutils.lakehouse.get(
                        value, workspaceId=self._workspace_id
                    )
                else:
                    lakehouse = notebookutils.lakehouse.get(value)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to resolve lakehouse '{value}' "
                    f"(workspace_id={self._workspace_id}): {e}"
                ) from e

            self._lakehouse_id = lakehouse["id"]
            if not self._workspace_id:
                self._workspace_id = lakehouse.get("workspaceId")

    @property
    def lakehouse_path(self) -> str:
        if not self._workspace_id or not self._lakehouse_id:
            raise ValueError("Both workspace_id and lakehouse_id must be resolved before building path.")
        return f"abfss://{self._workspace_id}@onelake.dfs.fabric.microsoft.com/{self._lakehouse_id}"

    def check_if_table_exists(self, table: str, schema: str) -> bool:
        path = self.build_delta_file_path(table, schema)
        return DeltaTable.isDeltaTable(spark, path)

    def build_delta_file_path(self, table: str, schema: str) -> str:
        return f"{self.lakehouse_path}/Tables/{schema}/{table}"

    def write_delta_table(
        self,
        data: DataFrame,
        schema: str,
        table: str,
        partition_by: Optional[Union[list, str]] = None,
        mode: str = "error",
        **kwargs,
    ) -> None:
        path = self.build_delta_file_path(table, schema)
        writer = data.write.format("delta").mode(mode).options(**kwargs)
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.save(path)

    def get_table_schema(self, table: str, schema: str) -> StructType:
        path = self.build_delta_file_path(table, schema)
        return spark.read.format("delta").load(path).schema

    def get_table_dtypes(self, table: str, schema: str) -> list:
        path = self.build_delta_file_path(table, schema)
        return spark.read.format("delta").load(path).dtypes
