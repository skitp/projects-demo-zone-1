def _ingest_cdc(self, source_data: DataFrame, base_data, merge_predicate: str) -> None:
    """
    Unified CDC path (ops 1/2/3/4).

    Per candidate key:
      - If any op 2 or 4 exists → upsert the latest 2/4 (ordinary update or insert).
      - If only op 1 or 3 exists → soft-delete (delete or before-image of a key change).
    """
    order_cols = [
        c for c in ("__start_lsn", "__command_id", "__seqval")
        if c in source_data.columns
    ]

    # --- Upsert set: latest op 2 or 4 per key ---
    upsert_src = source_data.filter(col("__operation").isin(2, 4))
    if order_cols and upsert_src.limit(1).count() > 0:
        w_up = Window.partitionBy(*self._candidate_keys).orderBy(
            *[col(c).desc() for c in order_cols]
        )
        to_upsert = (
            upsert_src.withColumn("_rn", row_number().over(w_up))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
    else:
        to_upsert = upsert_src

    # --- Delete set: keys whose latest overall op is 1 or 3 (no surviving 2/4) ---
    if order_cols:
        w_all = Window.partitionBy(*self._candidate_keys).orderBy(
            *[col(c).desc() for c in order_cols],
            col("__operation").desc(),  # tie-break: prefer 4 over 3
        )
        latest_any = (
            source_data.withColumn("_rn", row_number().over(w_all))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
    else:
        latest_any = source_data

    # Keys that will be upserted must not also be soft-deleted
    upsert_keys = to_upsert.select(*self._candidate_keys).distinct()
    to_delete = (
        latest_any.filter(col("__operation").isin(1, 3))
        .join(upsert_keys, on=self._candidate_keys, how="left_anti")
    )

    # Diagnostic
    self._log_unmatched(source_data)

    # 1. Soft-delete (true deletes + old keys from PK updates)
    if to_delete.limit(1).count() > 0:
        logger.info("CDC soft-delete phase (%s keys)", to_delete.count())
        (
            base_data.alias("bd")
            .merge(to_delete.alias("sd"), merge_predicate)
            .whenMatchedUpdate(
                set={
                    "dl_isdeleted": lit(1),
                    "dl_iscurrent": lit(0),
                    "dl_lastmodifiedutc": lit(self.batch_time),
                }
            )
            .execute()
        )

    # 2. Upsert (inserts + ordinary updates + after-images of PK changes)
    if to_upsert.limit(1).count() > 0:
        logger.info("CDC upsert phase (%s keys)", to_upsert.count())
        upd = self._get_upd_col(
            to_upsert,
            exclusions=[
                "dl_createddateutc",
                "__start_lsn",
                "__seqval",
                "__command_id",
                "__operation",
            ],
        )
        ins = self._get_upd_col(
            to_upsert,
            exclusions=["__operation", "__start_lsn", "__seqval", "__command_id"],
        )
        upd = {
            **upd,
            "dl_isdeleted": lit(0),
            "dl_iscurrent": lit(1),
            "dl_lastmodifiedutc": lit(self.batch_time),
        }
        ins = {**ins, "dl_isdeleted": lit(0), "dl_iscurrent": lit(1)}

        (
            base_data.alias("bd")
            .merge(to_upsert.alias("sd"), merge_predicate)
            .withSchemaEvolution()
            .whenMatchedUpdate(set=upd)
            .whenNotMatchedInsert(values=ins)
            .execute()
        )

    logger.info(
        "CDC merge finished. Compare active rows (dl_iscurrent=1) with source."
    )
