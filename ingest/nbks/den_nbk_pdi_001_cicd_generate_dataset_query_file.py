#!/usr/bin/env python
# coding: utf-8
"filterExpression": "name <> 'PAFAKE10'"
"filter_expression": "trancnt = 0 and after = 1
# ## den_nbk_pdi_001_cicd_generate_dataset_query_file
# 
# null

# In[ ]:


import fsspec
import json
import logging
from typing import List, Dict, Optional
from pathlib import Path


# In[ ]:


source_name = "sqlcrqry"


# In[ ]:


init_watermark_value = locals().get("init_watermark_value") or "1983-01-01T00:00:00Z"
top_rows = locals().get("top_rows")
source_name = locals().get("source_name")
datasets_folder = locals().get("datasets_folder") or "datasets"


# In[ ]:


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# In[ ]:


def validate_path(path: str) -> bool:
    """Validate if the given path is non-empty and properly formatted."""
    if not path or not isinstance(path, str):
        logger.error("Invalid path provided")
        return False
    return True


# In[ ]:


def list_folder_contents(path: str, result_list: Optional[List[Dict]] = None) -> List[Dict]:
    """
    Recursively list files in the specified path, excluding watermark folders.
    
    Args:
        path (str): Path to scan for files
        result_list (List[Dict], optional): List to store results
        
    Returns:
        List[Dict]: List of dictionaries containing folder and file names
    """
    if not validate_path(path):
        return []

    if result_list is None:
        result_list = []
    
    try:
        items = notebookutils.fs.ls(path)
        for item in items:
            if item.isDir:
                folder_name = item.name.rstrip('/')
                if folder_name != 'watermark':
                    logger.info(f"Processing folder: {folder_name}")
                    list_folder_contents(item.path, result_list)
            else:
                parent_folder = path.split('/')[-1] if '/' in path else ""
                if parent_folder != 'watermark':
                    result_list.append({"folderName": parent_folder, "fileName": item.name})
                    logger.debug(f"Added file: folderName={parent_folder}, fileName={item.name}")
    except Exception as e:
        logger.error(f"Error accessing {path}: {str(e)}")
    
    return result_list


# In[ ]:


def generate_watermark(
    source_config_folder_name: str,
    source_config_file_name: str,
    storage_options: Dict,
    init_watermark_value: str,
    top_rows: Optional[int] = None
) -> None:
    """
    Generate watermark JSON file for a dataset based on its configuration.
    
    Args:
        source_config_folder_name (str): Folder containing the source config
        source_config_file_name (str): Name of the config file
        storage_options (Dict): Filesystem storage options
        init_watermark_value (str): Initial watermark value
        top_rows (Optional[int]): Number of rows to limit in query
    """
    try:
        input_path = f"{source_config_folder_name}{source_config_file_name}"
        watermark_path = f"{source_config_folder_name}watermark/{source_config_file_name}"
        
        if not validate_path(input_path):
            raise ValueError("Invalid input path")

        onelake_fs = fsspec.filesystem("abfss", **storage_options)
        
        # Load dataset configuration
        with onelake_fs.open(input_path, "r") as f:
            dataset = json.load(f)
        
        source_system_properties = dataset.get("sourceSystemProperties", {})
        curated_properties = dataset.get("curatedProperties", {})
        
        # Initialize query parameters
        columns_list = ",".join(source_system_properties.get("includeSpecificColumns", ["*"]))
        where_clause = ""
        watermark_value = init_watermark_value
        source_watermark_identifier = ""
        query = ""

        if dataset.get("datasetTypeName") not in ["database", "file"]:
            logger.warning(f"Unsupported dataset type: {dataset.get('datasetTypeName')}")
            return

        if source_system_properties.get("ingestType") == "watermark":
            source_watermark_identifier = source_system_properties.get("sourceWatermarkIdentifier", "")
            if not source_watermark_identifier:
                raise ValueError("sourceWatermarkIdentifier is required for watermark ingest type")
                
            columns_list += f",{source_watermark_identifier} as dl_watermark"
            
            if onelake_fs.exists(watermark_path):
                with onelake_fs.open(watermark_path, "r", encoding="utf-8-sig") as f:
                    existing_watermark = json.load(f)
                    if existing_watermark.get("sourceWatermarkIdentifier") == source_watermark_identifier:
                        watermark_value = existing_watermark.get("watermarkValue", watermark_value)
            
            where_clause = f"WHERE {source_watermark_identifier} >= CAST('{watermark_value}' AS datetime2)"

        elif source_system_properties.get("ingestType") == "cdc":
            primary_key_list = curated_properties.get("primaryKeyList", [])
            if not primary_key_list:
                raise ValueError("Primary key list is required for CDC ingest type")
                
            cdc_columns = ["__$operation", "__$start_lsn", " __$command_id", "__$seqval"]
            columns_list = f"{columns_list},{','.join(cdc_columns)}" if columns_list != "*" else "*"
            
            source_watermark_identifier = "__$start_lsn"
            watermark_value = '0x0'
            
            if onelake_fs.exists(watermark_path):
                with onelake_fs.open(watermark_path, "r", encoding="utf-8-sig") as f:
                    existing_watermark = json.load(f)
                    if existing_watermark.get("sourceWatermarkIdentifier") == source_watermark_identifier:
                        watermark_value = existing_watermark.get("watermarkValue", watermark_value)
            
            partition_keys = ",".join(primary_key_list)
            table_name = f"[cdc].[{dataset.get('datasetSchema', 'dbo')}_{dataset['datasetName']}_CT]"
            
            query = f"""WITH LatestChanges AS (
                SELECT {columns_list}, 
                ROW_NUMBER() OVER (PARTITION BY {partition_keys} ORDER BY [__$start_lsn] DESC, __$command_id DESC, __$seqval DESC) AS rn 
                FROM {table_name} 
                WHERE [__$operation] IN (1, 2, 3, 4) AND __$start_lsn >= {watermark_value}
            )
            SELECT {columns_list} FROM LatestChanges WHERE rn = 1"""

        filter_expression = source_system_properties.get('filterExpression', '').strip()
        if source_system_properties.get("isDynamicQuery") and filter_expression:
            filter_expression = filter_expression.lower()
            filter_expression = filter_expression.replace("where ", "", 1).replace("and ", "", 1).lstrip()
            
            where_clause = f"WHERE {filter_expression}" if not where_clause else f"{where_clause} AND {filter_expression}"

        if source_system_properties.get("ingestType") == "cdc":
            if filter_expression:
                query = f"{query} AND {filter_expression}"
        else:
            # where_clause = f"WHERE {filter_expression}" if filter_expression else ""
            query = (
                f"SELECT {columns_list} "
                f"FROM {dataset.get('datasetSchema', 'dbo')}.{dataset['datasetName']} "
                f"{where_clause}"
            )

        # Add TOP clause if specified
        if top_rows is not None:
            query = query.replace("SELECT", f"SELECT TOP({top_rows}) ", 1)

        # Create watermark JSON
        json_watermark_query = {
            "query": query,
            "sourceWatermarkIdentifier": source_watermark_identifier,
            "watermarkValue": watermark_value
        }

        # Write watermark file
        with onelake_fs.open(watermark_path, "w") as json_file:
            json.dump(json_watermark_query, json_file, indent=4)
            
        logger.info(f"Watermark file generated at: {watermark_path}")

    except Exception as e:
        logger.error(f"Failed to generate watermark for {source_config_file_name}: {str(e)}")
        raise


# In[ ]:


def main():
    """Main function to orchestrate watermark generation."""
    try:
        # Set filesystem options
        storage_options = {
            "account_name": "onelake",
            "account_host": "onelake.dfs.fabric.microsoft.com",
        }

        # Get workspace and lakehouse IDs
        lakehouse = notebookutils.lakehouse.get("den_lhw_pdi_001_metadata")
        workspace_id = lakehouse["workspaceId"]
        lakehouse_id = lakehouse["id"]

        # Set configuration
        # init_watermark_value = '1983-01-01T00:00:00Z'
        # top_rows = 1000 # limit 1000 on dataset TOP rows or None
        datasets_path = (
            f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{lakehouse_id}/Files/{datasets_folder}"
            f"{'_' + source_name if source_name else ''}"
        )

        logger.info(f"Processing datasets from: {datasets_path}")
        
        # List dataset files
        contents = list_folder_contents(datasets_path)
        
        if not contents:
            logger.warning("No dataset files found")
            return

        # Process each dataset
        for item in contents:
            source_config_folder_name = f"{datasets_path}/{item['folderName']}/"
            source_config_file_name = item['fileName']
            logger.info(f"Generating watermark for: {source_config_file_name}")
            generate_watermark(
                source_config_folder_name,
                source_config_file_name,
                storage_options,
                init_watermark_value,
                top_rows
            )
            
        logger.info("Watermark generation completed successfully")

    except Exception as e:
        logger.error(f"Watermark generation failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()

