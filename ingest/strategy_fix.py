# ---------------------------------------------------------------------------
# On class LoadStrategy  (instance methods – do NOT add @staticmethod)
# ---------------------------------------------------------------------------

def _normalize_key_columns(self, df: DataFrame) -> DataFrame:
    """Trim string candidate keys so whitespace cannot prevent a match."""
    if df is None or not self._candidate_keys:
        return df

    dtypes = dict(df.dtypes)
    for key in self._candidate_keys:
        if key not in df.columns:
            logger.warning("Candidate key '%s' missing during normalization", key)
            continue
        if "string" in dtypes.get(key, "").lower():
            df = df.withColumn(key, trim(col(key)))
    return df


def _write_unmatched_keys(self, unmatched_df: DataFrame) -> None:
    """Write unmatched source keys to quarantine.<table>_unmatched."""
    if unmatched_df is None:
        return
    # Cheap emptiness check
    if hasattr(unmatched_df, "isEmpty") and unmatched_df.isEmpty():
        logger.info("No unmatched source keys to write")
        return

    table_name = self.table_path.rstrip("/").split("/")[-1]
    # Derive .../Tables/quarantine/<table>_unmatched
    try:
        from spark_engine.data_quality.quarantine import QuarantineStrategy
        base = QuarantineStrategy._derive_quarantine_path(self.table_path)
        diag_path = base + "_unmatched" if not base.endswith("_unmatched") else base
    except Exception:
        parts = self.table_path.rstrip("/").split("/")
        # Fallback: replace last segment
        diag_path = "/".join(parts[:-1]) + f"/quarantine/{table_name}_unmatched"

    keep = [c for c in self._candidate_keys if c in unmatched_df.columns]
    for extra in ("__operation", "__start_lsn", "__seqval", "dl_rowhash"):
        if extra in unmatched_df.columns and extra not in keep:
            keep.append(extra)

    out = (
        unmatched_df.select(*keep)
        .withColumn("dl_diagnostic_type", lit("unmatched_source_key"))
        .withColumn("dl_batch_time", lit(str(self.batch_time)))
        .withColumn("dl_captured_utc", lit(datetime.now(timezone.utc).isoformat()))
    )

    logger.warning("Writing unmatched keys to %s", diag_path)
    (
        out.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("delta.checkpoint.writeStatsAsStruct", "true")
        .save(diag_path)
    )


# ---------------------------------------------------------------------------
# Replace MergeLoad.ingest with this version
# ---------------------------------------------------------------------------

def ingest(self, source_data: DataFrame) -> None:
    logger.info("Merging data into: %s", self.table_path)
    if not self._candidate_keys:
        logger.warning("No candidate keys provided; skipping merge")
        return

    try:
        # Normalize source keys (must call the method)
        source_data = self._normalize_key_columns(source_data)

        base_data = DeltaTable.forPath(spark, self.table_path)
        merge_predicate = self._create_merge_predicate(self._candidate_keys)
        logger.info("Merge predicate: %s", merge_predicate)

        # Unmatched detection – guard .columns access
        key_cols = [c for c in self._candidate_keys if c in source_data.columns]
        if not key_cols:
            logger.warning("None of the candidate keys are present in source; skipping unmatched check")
        else:
            target_keys = (
                spark.read.format("delta")
                .load(self.table_path)
                .select(*key_cols)
            )
            target_keys = self._normalize_key_columns(target_keys)

            unmatched = source_data.join(target_keys, on=key_cols, how="left_anti")
            if not (hasattr(unmatched, "isEmpty") and unmatched.isEmpty()):
                # Force a small action only when we know there may be rows
                unmatched_count = unmatched.count()
                if unmatched_count > 0:
                    logger.warning(
                        "%s source key(s) do not match any target row",
                        unmatched_count,
                    )
                    self._write_unmatched_keys(unmatched)
                else:
                    logger.info("All source keys matched existing target rows")
            else:
                logger.info("All source keys matched existing target rows")

        # ----- existing CDC / non-CDC merge (unchanged logic) -----
        is_cdc = "__operation" in source_data.columns
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
                .whenMatchedUpdate(condition=reactivate_or_change, set=upd_col_cdc)
                .whenNotMatchedInsert(
                    condition="sd.__operation IN (2, 4)",
                    values=ins_col_cdc,
                )
                .execute()
            )
            logger.info("CDC merge completed")
        else:
            upd_col = self._get_upd_col(source_data, exclusions=["dl_createddateutc"])
            (
                base_data.alias("bd")
                .merge(source=source_data.alias("sd"), condition=merge_predicate)
                .withSchemaEvolution()
                .whenMatchedUpdate(condition=row_changed_condition, set=upd_col)
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("Non-CDC merge completed")

    except Exception as e:
        logger.error("Error during merge ingest: %s", e)
        raise
