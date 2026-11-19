import json
from datetime import datetime
from uuid import uuid4
from spark_engine.sparkengine import SparkEngine
from spark_engine.common.lakehouse import LakehouseManager
import notebookutils as nu
import fsspec
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time


feed_name = "demo_product"
run_id = "aa119f99-0559-4e95-8adc-f52adfdd6915"
elt_id = "08bdd3a3-ccc5-43ad-82d6-3e01b1841587"
elt_start_date_time = "11/07/2024 20:02:44"
product_name = "DEMO"
source_system = "Curated"
invocation_id = "aa119f99-0559-4e95-8adc-f52adfdd6915"
workspace_id = "ab08da5e-0f71-423b-a811-bd0af21f182b"
lh_metadata_id = "7c6d771a-3b6f-4042-8a89-1a885973a93c"
lh_observability_id = "61a6df22-f73b-48bc-855f-55f41065eb20"


processing_start_time = elt_start_date_time
# Lakehouse names
logs_lakehouse_name = "den_lhw_pdi_001_observability"
product_config_lakehouse_name = "den_lhw_pdi_001_metadata"

# Check for additional parameters
required_params = ["workspace_id", "lh_metadata_id", "lh_observability_id"]
missing_params = [param for param in required_params if param not in locals() or eval(param) is None or eval(param) == '']

# Construct abfss path for Feed file name
feeds_folder_path ="data_product/feeds"
feed_path = f"{feeds_folder_path}/{feed_name}.json"
feed_path_uri = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_metadata_id}/Files/{feed_path}"

# Zone and stage settings
if 'zone' in locals():
    zone_name = zone
    stage_name = "Share"
else:
    zone_name = "Product"
    stage_name = "Transformation"

# set storage options
storage_options = {
    "account_name": "onelake",
    "account_host": "onelake.dfs.fabric.microsoft.com",
}


def get_current_timestamp() -> object:
    return datetime.utcnow()

def get_file_location_url(lakehouse_name, file_relative_path) -> str:
    lakehouse_manager = LakehouseManager(lakehouse_name=lakehouse_name)
    lakehouse_files_path = f"{lakehouse_manager.lakehouse_path}/Files"
    return f"{lakehouse_files_path}/{file_relative_path}"


def send_message_to_logs(message_metadata: object, log_file_name: str, file_name: str) -> object:
    message = {
        "product_name": product_name,
        "feed_name": feed_name,
        "dataset_name": file_name,
        "source_system": source_system,
        "metadata": message_metadata,
        "zone": zone_name,
        "stage": stage_name,
        "orchestration_tool": "spark",
        "zone_start_date_time": str(processing_start_time),
        "zone_end_date_time": str(get_current_timestamp()),
        "elt_id": elt_id,
        "run_id": run_id,
        "invocation_id": invocation_id
    }
    output_message = json.dumps(message)
    try:
        nu.fs.put(log_file_name, output_message, True)
    except Exception as error:
        raise error

def process_file(file_info, missing_params):
    """Helper function to process a single file."""

    file_name = file_info["fileName"]
    model_config_folder_name = file_info["modelConfigFolderName"]

    # Construct file paths
    config_file_relative_path = f"data_product/{model_config_folder_name}/{file_name}.yaml"
    log_file_relative_path = f"Metadata_Logs/{uuid4()}.json"

    if not missing_params:
        log_file_name = get_file_location_url(logs_lakehouse_name, log_file_relative_path)
        product_config_path = get_file_location_url(product_config_lakehouse_name, config_file_relative_path)
        print(f"Constructing abfss path with LakehouseManager class for {file_name}")
    else:
        log_file_name = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_observability_id}/Files/{log_file_relative_path}"
        product_config_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_metadata_id}/Files/{config_file_relative_path}"
        print(f"Constructing abfss path with additional parameters for {file_name}: workspace_id={workspace_id}, lh_observability_id={lh_observability_id}, lh_metadata_id={lh_metadata_id}")

    # Process the data for the current file
    print(f"Processing config file: {file_name}")
    return process_data(
        product_config_path=product_config_path,
        product_name=product_name,
        feed_name=feed_name,
        file_name=file_name,
        elt_id=elt_id,
        run_id=run_id,
        processing_start_time=processing_start_time,
        log_file_name=log_file_name
    )


def process_data(product_config_path, product_name, feed_name, file_name, elt_id, run_id, processing_start_time, log_file_name):
    max_retries = 3
    retry_delay = 60  # seconds

    for attempt in range(max_retries):
        try:
            data = (
                SparkEngine.transform(product_config_path)
                .configure_transform(
                    product_name=product_name,
                    feed_name=feed_name,
                    dataset_name=file_name
                )
                .start_transform(elt_id=elt_id, run_id=run_id)
                .metrics()
            )
            message_metadata = {"runOutput": data if 'data' in locals() else {}}
            return 'Success' # Success, exit retry loop

        except Exception as error:
            # Check if the error is related to status code 429
            is_429 = False
            error_message = str(error).lower()

            # Check for 429 in message or exception attributes (adjust based on SparkEngine (Transform class) behavior)
            if "429" in error_message or "RequestBlocked" in error_message:
                is_429 = True

            if is_429 and attempt < max_retries - 1:
                print(f"Received 429 for {file_name}, attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue  # Retry after delay
            else:
                # Either not a 429 or max retries reached
                data = {
                    "ingestion": {
                        "error_message": str(error),
                        "startTime": str(processing_start_time)
                    }
                }
                print(f"Exception occurred while processing the data for {file_name}: {error}")
                raise error  # Re-raise the error for outer handling
        finally:
            message_metadata = {"runOutput": data if 'data' in locals() else {}}
            send_message_to_logs(message_metadata, log_file_name, file_name)
            print(f"Finally sent {file_name} to logs.")

def get_feed_json_payload(feed_file_uri: str) -> json:
    onelake_fs = fsspec.filesystem("abfss", **storage_options)
    try:
        payload = json.load(onelake_fs.open(feed_file_uri, "r"))
    except Exception as e:
        raise ValueError(
            "Error loading Feed file!", str(e)[:500]
        )  
    return payload


def get_spark_max_workers():
    '''Determine max_workers based on Spark environment'''
    try:
        # Check environment variable
        spark_cores = os.environ.get("SPARK_EXECUTOR_CORES")  # Set in cluster config
        if spark_cores:
            total_cores = int(spark_cores) * int(os.environ.get("SPARK_EXECUTOR_INSTANCES", 1))
            max_workers = max(1, total_cores - 1)  # Subtract 1 for headroom
            print(f"Detected {total_cores} Spark cores from environment, setting max_workers to {max_workers}")
            return max_workers

        # Fallback to a method based on CPU count (if available)
        import multiprocessing
        cpu_count = multiprocessing.cpu_count()
        max_workers = max(1, cpu_count - 1)
        print(f"Using CPU count {cpu_count}, setting max_workers to {max_workers}")
        return max_workers

    except Exception as e:
        print(f"Could not determine Spark cores, defaulting to max_workers=3: {e}")
        return 3  # Fallback to a safe default

# Get Feed file as JSON payload
payload = get_feed_json_payload(feed_file_uri=feed_path_uri)

# Get Spark's environment cores to set max workers  
max_workers = get_spark_max_workers()

for load_group_key, load_groups in payload.items():
    print(f"Processing {load_group_key}")
    for load_group in load_groups:
        files = load_group.get("files", [])
        if not files:
            print(f"No files to process in {load_group_key}")
            continue

        # Process files in parallel using ThreadPoolExecutor, stop on any error
        with ThreadPoolExecutor(max_workers) as executor:  # Adjust max_workers as needed
            future_to_file = {
                executor.submit(process_file, file_info, missing_params): file_info["fileName"]
                for file_info in files
            }
            has_error = False
            for future in as_completed(future_to_file):
                file_name = future_to_file[future]
                try:
                    result = future.result()
                    print(f"Completed processing {file_name} with result: {result}")
                except Exception as e:
                    print(f"Error processing {file_name}: {e}")
                    has_error = True
                    break
            if has_error:
                print(f"Stopping processesing due to error in {load_group_key}")
                raise Exception(f"Stopping processesing due to error in {load_group_key}")
