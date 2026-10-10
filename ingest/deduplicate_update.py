# Updated deduplicate method for strategy.py
# Replace the existing deduplicate() in LoadStrategy with this version.
# It only invokes quarantine logic when duplicate records actually exist.

def deduplicate(self, source_data: DataFrame, quarantine_type: str) -> DataFrame:
    """
    Deduplicates the source data based on candidate keys and quarantines
    duplicates only when they exist.
    """
    logger.info("Deduplicating data with quarantine_type: %s", quarantine_type)
    if not self._candidate_keys:
        logger.warning("No candidate keys provided; skipping deduplication")
        return source_data

    try:
        # Prefer CDC sequence so the latest event per key wins
        order_exprs = [col("dl_lastmodifiedutc").desc()]
        if "__start_lsn" in source_data.columns:
            order_exprs = [col("__start_lsn").desc()]
            if "__seqval" in source_data.columns:
                order_exprs.append(col("__seqval").desc())

        window = Window.partitionBy(*self._candidate_keys).orderBy(*order_exprs)
        df = source_data.withColumn("row_number", row_number().over(window))
        unique_data = df.filter(col("row_number") == 1).drop("row_number")
        duplicates = df.filter(col("row_number") > 1)

        # Only call quarantine when there are actual duplicates
        if duplicates.limit(1).count() > 0:   # cheap existence check
            logger.info("Duplicates detected; invoking quarantine")
            quarantine = QuarantineTypes.get_quarantine_type(quarantine_type)
            quarantine.configure(self.table_path)
            quarantine.quarantine(duplicates)
            self.quarantine_metrics = quarantine.quarantine_metrics
            logger.info(
                "Deduplication completed. Quarantined records written to %s",
                getattr(quarantine, "quarantine_path", "unknown"),
            )
        else:
            logger.info("No duplicates found; skipping quarantine")
            self.quarantine_metrics = None

        return unique_data
    except Exception as e:
        logger.error("Error during deduplication: %s", e)
        raise
