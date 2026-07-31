# Generate both sets while rows are still 'initiated'
create_dataset_files("cdc")
create_dataset_files()                 # incremental / base

# Then promote everything that was processed
update_request_status_to_ready()

def update_request_status_to_ready() -> None:
    """
    Mark all rows currently in status = 'initiated' as 'ready'
    in the source_dataset_request table.

    Intended usage (after both generators have run):

        create_dataset_files("cdc")
        create_dataset_files()               # incremental
        update_request_status_to_ready()

    This function is intentionally separate so that both CDC and
    incremental configs can be generated while the rows are still
    in 'initiated' state.

    Raises:
        Exception: Re-raises any failure that occurs during the
                   count or update operation after logging the error.
    """
    dl_table_path_sdr = f"{lhs_path_root}/Tables/catalog/source_dataset_request"

    try:
        # Count how many rows will be affected (for logging)
        count_df = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM delta.`{dl_table_path_sdr}`
            WHERE status = 'initiated'
        """)
        pending_count = count_df.collect()[0]["cnt"]

        if pending_count == 0:
            logger.info(
                "No 'initiated' rows found in source_dataset_request – nothing to update."
            )
            return

        # Perform the update
        update_sql = f"""
            UPDATE delta.`{dl_table_path_sdr}`
            SET status = 'ready'
            WHERE status = 'initiated'
        """
        spark.sql(update_sql)

        logger.info(
            f"Updated status to 'ready' for {pending_count} dataset(s) "
            f"in source_dataset_request"
        )

    except Exception as e:
        logger.error(
            f"Failed to update status to 'ready' in source_dataset_request "
            f"(table: {dl_table_path_sdr}). Error: {e}"
        )
        raise
