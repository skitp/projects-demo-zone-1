# Add these imports at the top of strategy.py if not already present
# from pyspark.sql.functions import trim

# ---------------------------------------------------------------------------
# Add these two helper methods to the LoadStrategy class
# ---------------------------------------------------------------------------

def _normalize_key_columns(self, df: DataFrame) -> DataFrame:
    """
    Explicit pre-merge normalization of candidate key columns.
    Currently trims string keys so leading/trailing whitespace cannot
    prevent a match. Extend here if other normalizations are required.
    """
    if not self._candidate_keys or df is None:
        return df

    dtypes = {name: dtype for name, dtype in df.dtypes}
    for key in self._candidate_keys:
        if key not in df.columns:
            logger.warning("Candidate key '%s' missing from DataFrame during normalization", key)
            continue
        if "string" in dtypes.get(key, "").lower():
            df = df.withColumn(key, trim(col(key)))
    return df


def _write_unmatched_keys(self, unmatched_df: DataFrame, suffix: str = "_unmatched") -> None:
    """
    Write source keys that did not match the target into a diagnostic table
    under the quarantine schema: quarantine.<table>_unmatched
    """
    if unmatched_df is None or (hasattr(unmatched_df, "isEmpty") and unmatched_df.isEmpty()):
        logger.info("No unmatched source keys to write")
        return

    # Re-use the same path derivation logic used by quarantine
    # (fallback if QuarantineStrategy is not importable)
    try:
        from spark_engine.data_quality.quarantine import QuarantineStrategy
        diag_path = QuarantineStrategy._derive_quarantine_path(self.table_path)
        # Turn .../quarantine/table into .../quarantine/table_unmatched
        if diag_path.endswith(self.table_path.split("/")[-1]):
            diag_path = diag_path + suffix
        else:
            diag_path = diag_path.rstrip("/") + suffix
    except Exception:
        # Simple fallback
        table_name = self.table_path.rstrip("/").split("/")[-1]
        base = self.table_path.rsplit("/", 1)[0]
        diag_path = f"{base}/quarantine/{table_name}{suffix}"

    logger.warning(
        "Writing %s unmatched source key(s) to diagnostic table: %s",
        unmatched_df.count(),
        diag_path,
    )

    # Keep only the keys + CDC operation + batch marker for easy diagnosis
    keep_cols = [c for c in self._candidate_keys if c in unmatched_df.columns]
    for extra in ("__operation", "__start_lsn", "__seqval", "dl_rowhash"):
        if extra in unmatched_df.columns:
            keep_cols.append(extra)

    out = (
        unmatched_df.select(*keep_cols)
        .withColumn("dl_diagnostic_type", lit("unmatched_source_key"))
        .withColumn("dl_batch_time", lit(self.batch_time))
        .withColumn("dl_captured_utc", lit(datetime.now(timezone.utc).isoformat()))
    )

    (
        out.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("delta.checkpoint.writeStatsAsStruct", "true")
        .save(diag_path)
    )


# ---------------------------------------------------------------------------
# Replace the entire MergeLoad.ingest method with this version
# ---------------------------------------------------------------------------

def ingest(self, source_data: DataFrame) -> None:
    """
    Ingests data by merging into the existing table.
    CDC path includes:
      - key normalization
      - predicate logging
      - emission of unmatched source keys to a diagnostic table
    """
    logger.info(f"Merging data into: {self.table_path}")
    if not self._candidate_keys:
        logger.warning("No candidate keys provided; skipping merge")
        return

    try:
        # ----- Pre-merge key normalization -----
        source_data = self._normalize_key_columns(source_data)

        base_data = DeltaTable.forPath(spark, self.table_path)
        merge_predicate = self._create_merge_predicate(self._candidate_keys)
        logger.info("Merge predicate: %s", merge_predicate)

        # ----- Detect unmatched source keys (before merge) -----
        target_keys = (
            spark.read.format("delta")
            .load(self.table_path)
            .select(*[c for c in self._candidate_keys if c in source_data.columns])
        )
        target_keys = self._normalize_key_columns(target_keys)

        unmatched = source_data.join(
            target_keys,
            on=self._candidate_keys,
            how="left_anti",
        )

        unmatched_count = unmatched.count()
        if unmatched_count > 0:
            logger.warning(
                "%s source key(s) do not match any target row – these will be inserted "
                "(or ignored if __operation=1). Writing diagnostic table.",
                unmatched_count,
            )
            self._write_unmatched_keys(unmatched)
        else:
            logger.info("All source keys matched existing target rows")

        # ----- Original CDC / non-CDC merge logic -----
        upd_col = self._get_upd_col(source_data, exclusions=["dl_createddateutc"])
        is_cdc = "__operation" in source_data.columns
        logger.debug(f"CDC detected: {is_cdc}")

        row_changed_condition = "NOT (bd.dl_rowhash <=> sd.dl_rowhash)"

        if is_cdc:
            upd_col_cdc = self._get_upd_col(
                source_data,
                exclusions=["dl_createddateutc", "__start_lsn", "__seqval", "__operation"],
            )
            ins_col_cdc = self._get_upd_col(
                source_data,
                exclusions=["__operation", "__start_lsn", "__seqval"],
            )

            # Force correct flags so reactivations cannot drift
            upd_col_cdc = {
                **upd_col_cdc,
                "dl_isdeleted": lit(0),
                "dl_iscurrent": lit(1),
                "dl_lastmodifiedutc": lit(self.batch_time),
            }
            ins_col_cdc = {
                **ins_col_cdc,
                "dl_isdeleted": lit(0),
                "dl_iscurrent": lit(1),
            }

            reactivate_or_change = (
                f"sd.__operation IN (2, 4) AND "
                f"({row_changed_condition} OR bd.dl_isdeleted = 1 OR bd.dl_iscurrent = 0)"
            )

            (
                base_data.alias("bd")
                .merge(source=source_data.alias("sd"), condition=merge_predicate)
                .withSchemaEvolution()
                .whenMatchedUpdate(
                    condition="sd.__operation = 1",
                    set={
                        "dl_isdeleted": lit(1),
                        "dl_iscurrent": lit(0),
                        "dl_lastmodifiedutc": lit(self.batch_time),
                    },
                )
                .whenMatchedUpdate(
                    condition=reactivate_or_change,
                    set=upd_col_cdc,
                )
                .whenNotMatchedInsert(
                    condition="sd.__operation IN (2, 4)",
                    values=ins_col_cdc,
                )
                .execute()
            )
            logger.info(
                "CDC merge completed. Compare active rows "
                "(dl_iscurrent = 1 / dl_isdeleted = 0) with the source."
            )
        else:
            (
                base_data.alias("bd")
                .merge(source=source_data.alias("sd"), condition=merge_predicate)
                .withSchemaEvolution()
                .whenMatchedUpdate(condition=row_changed_condition, set=upd_col)
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("Non-CDC merge completed successfully")

    except Exception as e:
        logger.error(f"Error during merge ingest: {str(e)}")
        raise
