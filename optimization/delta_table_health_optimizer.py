"""
Delta Table Health Metrics & Cost-Aware Auto-OPTIMIZE
Fabric Spark Runtime (pure Python – no Databricks widgets)

Fabric-native equivalent of sys.sp_get_table_health_metrics.
Scores tables on small-file pressure, file count, optimize age and
recent DML activity. Triggers OPTIMIZE only when the expected
performance benefit justifies the compute cost.

Compatible with: Microsoft Fabric Spark Runtime 1.3+, Delta Lake 3.x
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

# =============================================================================
# PARAMETERS – edit these values before running the notebook
# =============================================================================

CONFIG_PATH = "Files/table_health_config.json"          # path relative to lakehouse Files
TARGET_SCHEMAS = "iis_transaction,claims_transaction,stg_claims_transaction"
TABLE_FILTER = ""                                       # comma-separated table names or empty = all
DRY_RUN = True                                          # True = assess only, never run OPTIMIZE
FORCE_OPTIMIZE = False                                  # True = ignore allow_auto_optimize flag

# =============================================================================
# LOGGING & SPARK SESSION
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("DeltaTableHealth")

spark: SparkSession = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError(
        "No active SparkSession. Run this code inside a Microsoft Fabric Spark notebook."
    )

logger.info("Spark session ready | version=%s", spark.version)

# =============================================================================
# LOAD CONFIGURATION
# =============================================================================

def load_config(path: str) -> Dict[str, Any]:
    """
    Load table_health_config.json from the lakehouse Files area.
    Supports relative lakehouse paths and absolute paths.
    """
    candidates = [
        path,
        f"/lakehouse/default/{path}",
        f"/lakehouse/default/Files/{path.lstrip('Files/')}",
        f"Files/{path}" if not path.startswith("Files/") else path,
    ]

    last_err = None
    for candidate in candidates:
        try:
            with open(candidate, "r") as f:
                cfg = json.load(f)
            logger.info("Config loaded from %s", candidate)
            return cfg
        except Exception as e:
            last_err = e
            continue

    # Fallback via fsspec (OneLake abfss)
    try:
        import fsspec
        with fsspec.open(path, "r") as f:
            cfg = json.load(f)
        logger.info("Config loaded via fsspec from %s", path)
        return cfg
    except Exception:
        pass

    raise FileNotFoundError(
        f"Could not load config from any candidate path. Last error: {last_err}. "
        f"Tried: {candidates}"
    )


CONFIG = load_config(CONFIG_PATH)

REQUIRED_SECTIONS = ["assessment", "recommendation_thresholds", "optimization_rules", "runtime"]
for section in REQUIRED_SECTIONS:
    if section not in CONFIG:
        raise ValueError(f"Missing required config section: {section}")

OPT_RULES = CONFIG["optimization_rules"]
THRESHOLDS = CONFIG["recommendation_thresholds"]
RUNTIME = CONFIG["runtime"]
DML_LOOKBACK_DAYS = OPT_RULES.get("dml_lookback_days", 14)
HISTORY_LIMIT = OPT_RULES.get("history_limit", 500)

logger.info(
    "Config loaded | auto_optimize=%s | min_size_gb=%.1f | min_files=%d | max_avg_mb=%.0f | threshold=%d",
    OPT_RULES.get("allow_auto_optimize", False),
    OPT_RULES["minimum_table_size_gb"],
    OPT_RULES["minimum_file_count"],
    OPT_RULES["maximum_average_file_size_mb"],
    OPT_RULES["auto_optimize_threshold"]
)

# =============================================================================
# SCORING FUNCTIONS
# =============================================================================

def resolve_score(value: float, rules: List[Dict], key: str) -> int:
    """Return the score of the first rule whose threshold is >= value."""
    for rule in rules:
        if value <= rule[key]:
            return rule["score"]
    return 0


def calculate_health_score(
    avg_file_size_mb: float,
    num_files: int,
    days_since_optimize: int,
    recent_dml_count: int
) -> Tuple[int, List[str]]:
    """
    Compute composite health score and human-readable reasons.
    Higher score = worse health (more urgent to OPTIMIZE).
    """
    score = 0
    reasons: List[str] = []

    # Small-file pressure
    file_size_rules = CONFIG["assessment"]["small_file_assessment"]
    file_size_score = 0
    for rule in file_size_rules:
        if avg_file_size_mb <= rule["maxAvgFileSizeMB"]:
            file_size_score = rule["score"]
            if file_size_score > 0:
                reasons.append(
                    f"{rule['severity']} small-file condition "
                    f"({avg_file_size_mb:.1f} MB avg)"
                )
            break
    score += file_size_score

    # File count
    file_count_score = resolve_score(
        num_files,
        CONFIG["assessment"]["file_count_assessment"],
        "maxFileCount"
    )
    score += file_count_score
    if file_count_score > 0:
        reasons.append(f"High file count ({num_files:,})")

    # Days since last OPTIMIZE
    age_score = resolve_score(
        days_since_optimize,
        CONFIG["assessment"]["optimize_age_assessment"],
        "maxDays"
    )
    score += age_score
    if age_score > 0:
        reasons.append(f"{days_since_optimize} days since OPTIMIZE")

    # Recent DML activity
    dml_score = resolve_score(
        recent_dml_count,
        CONFIG["assessment"]["dml_activity_assessment"],
        "maxOperations"
    )
    score += dml_score
    if dml_score > 0:
        reasons.append(
            f"{recent_dml_count} MERGE/UPDATE/DELETE operations "
            f"(last {DML_LOOKBACK_DAYS}d)"
        )

    return score, reasons


def get_recommendation(score: int) -> str:
    if score >= THRESHOLDS["optimize_soon"]:
        return "OPTIMIZE_NOW"
    elif score >= THRESHOLDS["monitor"]:
        return "OPTIMIZE_SOON"
    elif score >= THRESHOLDS["healthy"]:
        return "MONITOR"
    else:
        return "HEALTHY"


def should_optimize(
    score: int,
    size_gb: float,
    avg_file_size_mb: float,
    num_files: int
) -> bool:
    """
    Decide whether the expected benefit of OPTIMIZE justifies the compute cost.
    All gates must pass.
    """
    if size_gb < OPT_RULES["minimum_table_size_gb"]:
        return False
    if num_files < OPT_RULES["minimum_file_count"]:
        return False
    if avg_file_size_mb > OPT_RULES["maximum_average_file_size_mb"]:
        return False
    return score >= OPT_RULES["auto_optimize_threshold"]

# =============================================================================
# METRIC COLLECTION
# =============================================================================

def get_table_detail(full_table_name: str) -> Dict[str, Any]:
    """Return size, file count and average file size from DESCRIBE DETAIL."""
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0]
        size_bytes = detail["sizeInBytes"] or 0
        num_files = detail["numFiles"] or 0
        avg_mb = (size_bytes / num_files / (1024 * 1024)) if num_files > 0 else 0.0
        return {
            "size_bytes": size_bytes,
            "size_gb": size_bytes / (1024 ** 3),
            "num_files": num_files,
            "avg_file_size_mb": avg_mb,
            "location": detail.get("location"),
            "format": detail.get("format")
        }
    except Exception as e:
        logger.error("DESCRIBE DETAIL failed for %s: %s", full_table_name, str(e))
        raise


def get_history_metrics(full_table_name: str) -> Dict[str, Any]:
    """
    Analyse DESCRIBE HISTORY to obtain:
      - days since last OPTIMIZE
      - count of recent DML operations (MERGE / UPDATE / DELETE / WRITE)
    """
    try:
        hist_df = spark.sql(
            f"DESCRIBE HISTORY {full_table_name} LIMIT {HISTORY_LIMIT}"
        )

        # Last OPTIMIZE
        optimize_rows = (
            hist_df
            .filter(col("operation") == "OPTIMIZE")
            .orderBy(col("timestamp").desc())
            .limit(1)
            .collect()
        )
        if optimize_rows:
            last_opt_ts = optimize_rows[0]["timestamp"]
            days_since = (
                datetime.now(timezone.utc) - last_opt_ts.replace(tzinfo=timezone.utc)
            ).days
        else:
            days_since = 9999  # never optimized

        # Recent DML
        cutoff = datetime.now(timezone.utc) - timedelta(days=DML_LOOKBACK_DAYS)
        dml_ops = {
            "MERGE", "UPDATE", "DELETE", "WRITE",
            "CREATE OR REPLACE TABLE AS SELECT", "CREATE TABLE AS SELECT"
        }
        recent_dml = (
            hist_df
            .filter(col("timestamp") >= lit(cutoff))
            .filter(col("operation").isin(list(dml_ops)))
            .count()
        )

        return {
            "days_since_optimize": days_since,
            "recent_dml_count": recent_dml,
            "last_optimize_ts": optimize_rows[0]["timestamp"] if optimize_rows else None
        }
    except Exception as e:
        logger.warning(
            "History analysis failed for %s: %s. Using conservative defaults.",
            full_table_name, str(e)
        )
        return {
            "days_since_optimize": 9999,
            "recent_dml_count": 0,
            "last_optimize_ts": None
        }


def collect_table_metrics(full_table_name: str) -> Dict[str, Any]:
    """Combine detail + history into a single metrics dict."""
    detail = get_table_detail(full_table_name)
    history = get_history_metrics(full_table_name)
    return {**detail, **history}

# =============================================================================
# METRICS TABLE MANAGEMENT
# =============================================================================

METRICS_SCHEMA = RUNTIME["metrics_schema"]
METRICS_TABLE = RUNTIME["metrics_table"]
FULL_METRICS_TABLE = f"{METRICS_SCHEMA}.{METRICS_TABLE}"

METRICS_DDL = f"""
CREATE TABLE IF NOT EXISTS {FULL_METRICS_TABLE} (
  assessment_date          TIMESTAMP,
  table_name               STRING,
  size_gb                  DOUBLE,
  num_files                BIGINT,
  avg_file_size_mb         DOUBLE,
  days_since_optimize      INT,
  recent_dml_count         INT,
  health_score             INT,
  recommendation           STRING,
  optimize_required        BOOLEAN,
  optimize_executed        BOOLEAN,
  reason_codes             ARRAY<STRING>,
  assessment_duration_sec  DOUBLE,
  notebook_run_id          STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
"""


def ensure_metrics_table() -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METRICS_SCHEMA}")
    spark.sql(METRICS_DDL)
    logger.info("Metrics table ready: %s", FULL_METRICS_TABLE)


def upsert_assessment(record: Row) -> None:
    """Idempotent daily upsert: one row per table per calendar day."""
    source_df = spark.createDataFrame([record])
    target = DeltaTable.forName(spark, FULL_METRICS_TABLE)

    (
        target.alias("t")
        .merge(
            source_df.alias("s"),
            """
            t.table_name = s.table_name
            AND CAST(t.assessment_date AS DATE) = CAST(s.assessment_date AS DATE)
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# =============================================================================
# OPTIMIZE EXECUTION
# =============================================================================

def run_optimize(full_table_name: str, dry_run: bool = True) -> bool:
    """
    Execute OPTIMIZE only when explicitly allowed.
    Returns True if OPTIMIZE was actually run.
    """
    if dry_run:
        logger.info("[DRY-RUN] Would execute: OPTIMIZE %s", full_table_name)
        return False

    logger.info("Executing OPTIMIZE on %s ...", full_table_name)
    start = datetime.now(timezone.utc)
    try:
        spark.sql(f"OPTIMIZE {full_table_name}")
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info("OPTIMIZE completed for %s in %.1f s", full_table_name, duration)
        return True
    except Exception as e:
        logger.error("OPTIMIZE failed for %s: %s", full_table_name, str(e))
        return False

# =============================================================================
# TABLE DISCOVERY
# =============================================================================

def discover_tables(schemas: List[str], table_filter: str = "") -> List[str]:
    """
    Return fully-qualified Delta table names that match the requested schemas
    and optional filter.
    """
    tables = []
    filter_set = {
        t.strip().lower() for t in table_filter.split(",") if t.strip()
    } if table_filter else set()

    for schema in schemas:
        schema = schema.strip()
        if not schema:
            continue
        try:
            rows = spark.sql(f"SHOW TABLES IN {schema}").collect()
            for r in rows:
                tbl = r["tableName"]
                full = f"{schema}.{tbl}"
                if filter_set and tbl.lower() not in filter_set and full.lower() not in filter_set:
                    continue
                # Only process Delta tables
                try:
                    fmt = spark.sql(f"DESCRIBE DETAIL {full}").collect()[0]["format"]
                    if fmt and fmt.lower() == "delta":
                        tables.append(full)
                except Exception:
                    logger.debug("Skipping non-Delta or inaccessible table: %s", full)
        except Exception as e:
            logger.warning("Could not list tables in schema %s: %s", schema, str(e))

    logger.info("Discovered %d Delta tables for assessment", len(tables))
    return sorted(tables)

# =============================================================================
# MAIN ASSESSMENT LOOP
# =============================================================================

def assess_single_table(
    full_table_name: str,
    dry_run: bool,
    force_optimize: bool,
    run_id: str
) -> Optional[Row]:
    """
    Full health assessment for one table.
    Returns a Row ready for the metrics table, or None on fatal error.
    """
    start = datetime.now(timezone.utc)
    logger.info("-" * 80)
    logger.info("Assessing: %s", full_table_name)

    try:
        metrics = collect_table_metrics(full_table_name)

        score, reasons = calculate_health_score(
            avg_file_size_mb=metrics["avg_file_size_mb"],
            num_files=metrics["num_files"],
            days_since_optimize=metrics["days_since_optimize"],
            recent_dml_count=metrics["recent_dml_count"]
        )

        recommendation = get_recommendation(score)
        optimize_req = should_optimize(
            score=score,
            size_gb=metrics["size_gb"],
            avg_file_size_mb=metrics["avg_file_size_mb"],
            num_files=metrics["num_files"]
        )

        allow_auto = OPT_RULES.get("allow_auto_optimize", False) or force_optimize
        will_optimize = optimize_req and allow_auto and not dry_run

        logger.info(
            "  size=%.2f GB | files=%d | avg=%.1f MB | days_since_opt=%d | dml=%d",
            metrics["size_gb"], metrics["num_files"], metrics["avg_file_size_mb"],
            metrics["days_since_optimize"], metrics["recent_dml_count"]
        )
        logger.info(
            "  score=%d | recommendation=%s | optimize_required=%s",
            score, recommendation, optimize_req
        )
        for r in reasons:
            logger.info("    • %s", r)

        optimize_executed = False
        if will_optimize:
            optimize_executed = run_optimize(full_table_name, dry_run=False)
        elif optimize_req and dry_run:
            logger.info(
                "  [DRY-RUN] OPTIMIZE would be executed (score=%d >= threshold)", score
            )
        elif optimize_req and not allow_auto:
            logger.info(
                "  OPTIMIZE warranted but allow_auto_optimize=false "
                "(and force_optimize=false)"
            )

        duration = (datetime.now(timezone.utc) - start).total_seconds()

        return Row(
            assessment_date=datetime.now(timezone.utc),
            table_name=full_table_name,
            size_gb=round(metrics["size_gb"], 3),
            num_files=int(metrics["num_files"]),
            avg_file_size_mb=round(metrics["avg_file_size_mb"], 2),
            days_since_optimize=int(metrics["days_since_optimize"]),
            recent_dml_count=int(metrics["recent_dml_count"]),
            health_score=int(score),
            recommendation=recommendation,
            optimize_required=bool(optimize_req),
            optimize_executed=bool(optimize_executed),
            reason_codes=reasons,
            assessment_duration_sec=round(duration, 2),
            notebook_run_id=run_id
        )

    except Exception as e:
        logger.exception("Failed to assess %s: %s", full_table_name, str(e))
        return None


def main() -> None:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logger.info("=" * 80)
    logger.info(
        "Delta Table Health Assessment started | run_id=%s | dry_run=%s | force_optimize=%s",
        run_id, DRY_RUN, FORCE_OPTIMIZE
    )
    logger.info("=" * 80)

    ensure_metrics_table()

    schemas = [s.strip() for s in TARGET_SCHEMAS.split(",") if s.strip()]
    if not schemas:
        schemas = [r["namespace"] for r in spark.sql("SHOW SCHEMAS").collect()]
        logger.info("No schemas specified – assessing all accessible schemas: %s", schemas)

    table_list = discover_tables(schemas, TABLE_FILTER)
    if not table_list:
        logger.warning("No Delta tables found. Exiting.")
        return

    results: List[Row] = []
    for tbl in table_list:
        row = assess_single_table(
            tbl,
            dry_run=DRY_RUN,
            force_optimize=FORCE_OPTIMIZE,
            run_id=run_id
        )
        if row is not None:
            results.append(row)
            try:
                upsert_assessment(row)
            except Exception as e:
                logger.error("Failed to upsert metrics for %s: %s", tbl, str(e))

    # Summary
    logger.info("=" * 80)
    logger.info("Assessment complete | tables_processed=%d", len(results))
    if results:
        now = [r for r in results if r.recommendation == "OPTIMIZE_NOW"]
        soon = [r for r in results if r.recommendation == "OPTIMIZE_SOON"]
        executed = [r for r in results if r.optimize_executed]
        logger.info("  OPTIMIZE_NOW   : %d", len(now))
        logger.info("  OPTIMIZE_SOON  : %d", len(soon))
        logger.info("  OPTIMIZE run   : %d", len(executed))
        if now:
            logger.info("Tables requiring immediate attention:")
            for r in sorted(now, key=lambda x: -x.health_score):
                logger.info(
                    "  %-50s score=%3d  %s",
                    r.table_name, r.health_score, ", ".join(r.reason_codes[:2])
                )
    logger.info("=" * 80)


# =============================================================================
# EXECUTE
# =============================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Fatal error in table health notebook: %s", str(e))
        raise
else:
    # When the cell is run inside a Fabric notebook the module name is not __main__
    try:
        main()
    except Exception as e:
        logger.exception("Fatal error in table health notebook: %s", str(e))
        raise
