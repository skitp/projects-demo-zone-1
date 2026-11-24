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
        self.lakehouse_path = f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_id}"

    @property
    def workspace_id(self):
        return self._workspace_id

    @workspace_id.setter
    def workspace_id(self, value: str = None):
        """
        Allows the setting of the workspace id manually instead of getting it automatically using notebookutils.
        """
        if value and re.match("\w{8}-\w{4}-\w{4}-\w{4}-\w{12}", value):
            self._workspace_id = value
        else:
            self._workspace_id = None

    @property
    def lakehouse_id(self):
        return self._lakehouse_id

    @lakehouse_id.setter
    def lakehouse_id(self, value: str = None):
        """
        Allows setting of the lakehouse id manually or getting it automatically using notebookutils.
        The workspace id must be set using an id if an id is used for the lakehouse id.
        """
        if re.match("\w{8}-\w{4}-\w{4}-\w{4}-\w{12}", value):
            if not self._workspace_id:
                raise ValueError(
                    "Workspace Id is missing. It must be set when manually passing the Lakehouse Id."
                )
            self._lakehouse_id = value
        else:
            lakehouse = notebookutils.lakehouse.get(value)
            if not self._workspace_id:
                self._workspace_id = lakehouse["workspaceId"]
            self._lakehouse_id = lakehouse["id"]


    def check_if_table_exists(self, table: str, schema:str):
        path = self.build_delta_file_path(table, schema)
        return DeltaTable.isDeltaTable(spark, path)
    
    def build_delta_file_path(self, table: str, schema: str):
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

        if partition_by is None:
            (
                data.write.format("delta")
                .mode(mode)
                .options(**kwargs)
                .save(path)
            )
        else:
            (
                data.write.format("delta")
                .mode(mode)
                .partitionBy(partition_by)
                .options(**kwargs)
                .save(path)
            )

    def get_table_schema(self, table: str, schema: str) -> StructType:
        path = self.build_delta_file_path(table, schema)
        return spark.read.format("delta").load(path).schema
    
    def get_table_dtypes(self, table: str, schema: str) -> list:
        path = self.build_delta_file_path(table, schema)
        return spark.read.format("delta").load(path).dtypes
        
class SchemaManager:
    def clean_schema_name(self, name: str) -> str:
        return strip_string(name, "", True)

    def clean_table_name(self, name: str) -> str:
        return strip_string(name, "", True)
