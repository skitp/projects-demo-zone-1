"""
Backfill / seed utility for the durable identity key map.

Seeds {schema}.map_{table}_identity from an existing dimension so current
surrogate keys are preserved before switching to IdentityKeyMap.resolve().

Frozen hash recipe (must match IdentityKeyMap.bk_hash_expr):
    sha2(concat_ws('||', coalesce(cast(k as string), '') for k in candidate_keys), 256)

Rules
-----
- Never update an existing map identity for a bk_hash.
- Never seed reserved identities (-1 unknown, -2 not applicable).
- One identity per business key. If the dimension has more than one SK for
  the same candidate keys (SCD2 history, or dirty data):
    1. Prefer the current version (dl_is_current_flag = true) when present.
    2. Otherwise take the minimum positive identity.
    Conflicts are written to a diagnostics DataFrame and counted.
- dry_run=True is the default. Pass dry_run=False only after counts look right.

Example (Fabric notebook)
-------------------------
from seed_identity_map import seed_identity_map

summary = seed_identity_map(
    spark=spark,
    dim_path=lh.build_delta_file_path("dim_adjuster", "claims_transaction"),
    map_path=lh.build_delta_file_path("map_dim_adjuster_identity", "claims_transaction"),
    candidate_keys=["adjustr_bus_key"],
    identity_col="adjustr_key",
    elt_id="seed-dim-adjuster",
    run_id="2026-11-09",
    dry_run=True,
)
print(summary)
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


RESERVED_UNKNOWN = -1
RESERVED_NOT_APPLICABLE = -2
IDENTITY_FLOOR = 0


@dataclass
class SeedSummary:
    dry_run: bool
    dim_path: str
    map_path: str
    map_existed: bool
    dim_distinct_bk: int
    dim_positive_sk_rows: int
    reserved_excluded: int
    conflicts: int
    preferred_current_used: int
    rows_to_seed: int
    already_in_map: int
    inserted: int
    max_identity_after: Optional[int]
    message: str

    def as_dict(self) -> Dict:
        return asdict(self)


def bk_hash_expr(candidate_keys: List[str]):
    """Same recipe as IdentityKeyMap.bk_hash_expr. Do not change after go-live."""
    cols = [F.coalesce(F.col(name).cast("string"), F.lit("")) for name in candidate_keys]
    return F.sha2(F.concat_ws("||", *cols), 256)


def _table_exists(spark: SparkSession, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def _count(df: DataFrame) -> int:
    return df.count()


def build_seed_candidates(
    dim_df: DataFrame,
    candidate_keys: List[str],
    identity_col: str,
) -> tuple[DataFrame, DataFrame, Dict[str, int]]:
    """
    Collapse the dimension to one identity per business key.

    Preference:
      1. current row (dl_is_current_flag is true) if that column exists
      2. otherwise the minimum positive identity
    """
    missing = [c for c in (*candidate_keys, identity_col) if c not in dim_df.columns]
    if missing:
        raise ValueError(f"Dimension is missing required columns: {missing}")

    base = dim_df.select(*candidate_keys, identity_col, *[
        c for c in ("dl_is_current_flag",) if c in dim_df.columns
    ])

    reserved = base.filter(F.col(identity_col).isin(RESERVED_UNKNOWN, RESERVED_NOT_APPLICABLE))
    positive = base.filter(F.col(identity_col) > IDENTITY_FLOOR)

    positive = positive.withColumn("bk_hash", bk_hash_expr(candidate_keys))

    # Drop all-null business keys
    positive = positive.dropna(how="all", subset=candidate_keys)

    has_current = "dl_is_current_flag" in positive.columns
    if has_current:
        positive = positive.withColumn(
            "_prefer",
            F.when(F.col("dl_is_current_flag") == F.lit(True), F.lit(0)).otherwise(F.lit(1)),
        )
        w = Window.partitionBy("bk_hash").orderBy(F.col("_prefer"), F.col(identity_col).asc())
    else:
        w = Window.partitionBy("bk_hash").orderBy(F.col(identity_col).asc())

    ranked = positive.withColumn("_rn", F.row_number().over(w))
    chosen = ranked.filter(F.col("_rn") == 1).drop("_rn", "_prefer")

    # Conflicts: more than one positive SK for the same hash
    sk_counts = (
        positive.groupBy("bk_hash")
        .agg(F.countDistinct(identity_col).alias("sk_cnt"))
    )
    conflicts = (
        positive.join(sk_counts.filter(F.col("sk_cnt") > 1), on="bk_hash", how="inner")
        .select("bk_hash", *candidate_keys, identity_col, "sk_cnt")
        .distinct()
    )

    preferred_current_used = 0
    if has_current:
        preferred_current_used = _count(chosen.filter(F.col("dl_is_current_flag") == F.lit(True)))

    stats = {
        "dim_distinct_bk": _count(dim_df.select(*candidate_keys).dropna(how="all").distinct()),
        "dim_positive_sk_rows": _count(positive.select("bk_hash").distinct()),
        "reserved_excluded": _count(reserved),
        "conflicts": _count(sk_counts.filter(F.col("sk_cnt") > 1)),
        "preferred_current_used": preferred_current_used,
    }
    return chosen, conflicts, stats


def seed_identity_map(
    spark: SparkSession,
    dim_path: str,
    map_path: str,
    candidate_keys: List[str],
    identity_col: str,
    elt_id: str = "seed",
    run_id: Optional[str] = None,
    batch_time: Optional[str] = None,
    dry_run: bool = True,
) -> SeedSummary:
    """
    Seed the map from an existing dimension.

    Parameters
    ----------
    dim_path : Delta path of the source dimension
    map_path : Delta path of {schema}.map_{table}_identity
    candidate_keys : business key columns (same list used by IdentityKeyMap)
    identity_col : published surrogate column (e.g. adjustr_key)
    dry_run : if True, compute counts and return without writing
    """
    if not candidate_keys:
        raise ValueError("candidate_keys is required")
    if not dim_path or not map_path:
        raise ValueError("dim_path and map_path are required")

    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    batch_time = batch_time or datetime.now(timezone.utc).isoformat()

    if not _table_exists(spark, dim_path):
        raise FileNotFoundError(f"Dimension is not a Delta table: {dim_path}")

    dim_df = spark.read.format("delta").load(dim_path)
    chosen, conflicts, stats = build_seed_candidates(dim_df, candidate_keys, identity_col)

    map_existed = _table_exists(spark, map_path)
    if map_existed:
        existing = spark.read.format("delta").load(map_path).select("bk_hash").distinct()
        to_insert = chosen.join(existing, on="bk_hash", how="left_anti")
        already = _count(chosen.join(existing, on="bk_hash", how="left_semi"))
    else:
        to_insert = chosen
        already = 0

    rows_to_seed = _count(to_insert)
    inserted = 0
    max_after: Optional[int] = None
    message = "dry-run only; no writes"

    if not dry_run and rows_to_seed > 0:
        payload = (
            to_insert.select("bk_hash", *candidate_keys, F.col(identity_col).cast("long").alias(identity_col))
            .withColumn("dl_map_insert_ts", F.lit(batch_time).cast("timestamp"))
            .withColumn("dl_elt_id", F.lit(elt_id))
            .withColumn("dl_run_id", F.lit(run_id))
        )

        if not map_existed:
            (
                payload.write.format("delta")
                .mode("overwrite")
                .option("delta.checkpoint.writeStatsAsStruct", "true")
                .save(map_path)
            )
            inserted = rows_to_seed
            map_existed = True
        else:
            (
                DeltaTable.forPath(spark, map_path)
                .alias("target")
                .merge(payload.alias("source"), "source.bk_hash = target.bk_hash")
                .whenNotMatchedInsertAll()
                .execute()
            )
            inserted = rows_to_seed

        max_row = (
            spark.read.format("delta").load(map_path)
            .select(F.max(F.col(identity_col)).alias("m"))
            .collect()[0]["m"]
        )
        max_after = int(max_row) if max_row is not None else None
        message = f"seeded {inserted} new map rows"
    elif not dry_run and rows_to_seed == 0:
        message = "nothing to seed; map already covers all positive business keys"
        if map_existed:
            max_row = (
                spark.read.format("delta").load(map_path)
                .select(F.max(F.col(identity_col)).alias("m"))
                .collect()[0]["m"]
            )
            max_after = int(max_row) if max_row is not None else None
    elif dry_run and map_existed:
        max_row = (
            spark.read.format("delta").load(map_path)
            .select(F.max(F.col(identity_col)).alias("m"))
            .collect()[0]["m"]
        )
        max_after = int(max_row) if max_row is not None else None

    return SeedSummary(
        dry_run=dry_run,
        dim_path=dim_path,
        map_path=map_path,
        map_existed=map_existed,
        dim_distinct_bk=stats["dim_distinct_bk"],
        dim_positive_sk_rows=stats["dim_positive_sk_rows"],
        reserved_excluded=stats["reserved_excluded"],
        conflicts=stats["conflicts"],
        preferred_current_used=stats["preferred_current_used"],
        rows_to_seed=rows_to_seed,
        already_in_map=already,
        inserted=inserted,
        max_identity_after=max_after,
        message=message,
    )


def seed_identity_map_via_lakehouse(
    spark: SparkSession,
    lakehouse,
    schema: str,
    dimension_table: str,
    identity_col: str,
    candidate_keys: List[str],
    elt_id: str = "seed",
    run_id: Optional[str] = None,
    dry_run: bool = True,
    map_table: Optional[str] = None,
    map_schema: Optional[str] = None,
) -> SeedSummary:
    """Wrapper that resolves paths through LakehouseManager."""
    map_schema = map_schema or schema
    map_table = map_table or f"map_{dimension_table}_identity"
    dim_path = lakehouse.build_delta_file_path(dimension_table, schema)
    map_path = lakehouse.build_delta_file_path(map_table, map_schema)
    return seed_identity_map(
        spark=spark,
        dim_path=dim_path,
        map_path=map_path,
        candidate_keys=candidate_keys,
        identity_col=identity_col,
        elt_id=elt_id,
        run_id=run_id,
        dry_run=dry_run,
    )
