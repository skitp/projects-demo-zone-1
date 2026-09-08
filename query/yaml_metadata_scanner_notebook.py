# =====================================================================
# Notebook: YAML metadata scanner (PII + SQL source columns)
# Purpose:  Scan OneLake YAML data-product configs and persist:
#           1) declared target.pii_columns
#           2) source table columns referenced in query[].sql
# Runtime:  Microsoft Fabric notebook (notebookutils + Spark + Delta)
# Author:   Serge
# Updated:  2026-09-08
# =====================================================================

from __future__ import annotations

import logging
import posixpath
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import fsspec
import yaml
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, explode, lit

# ────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────
METADATA_LAKEHOUSE_NAME = "den_lhw_pdi_001_metadata"
OBSERVABILITY_LAKEHOUSE_NAME = "den_lhw_pdi_001_observability"
OUTPUT_SCHEMA = "audit"
PII_TABLE_NAME = "yaml_pii_columns_config"
SOURCE_COLUMNS_TABLE_NAME = "yaml_sql_source_columns"
YAML_FILES_SUBPATH = "Files/data_product"

STORAGE_OPTIONS = {
    "account_name": "onelake",
    "account_host": "onelake.dfs.fabric.microsoft.com",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("yaml_metadata_scanner")

spark = SparkSession.builder.appName("YAML_Metadata_Scanner").getOrCreate()

SQL_KEYWORDS = {
    "select", "from", "where", "join", "inner", "left", "right", "full", "cross",
    "outer", "on", "and", "or", "not", "as", "distinct", "union", "all", "order",
    "by", "group", "having", "limit", "offset", "with", "over", "partition",
    "rows", "unbounded", "preceding", "following", "current", "row", "case",
    "when", "then", "else", "end", "between", "in", "is", "null", "like",
    "exists", "cast", "coalesce", "concat", "upper", "lower", "trim", "replace",
    "year", "month", "day", "first_value", "row_number", "lpad", "true", "false",
    "asc", "desc", "using", "natural", "lateral", "anti", "semi", "values",
}
SQL_TYPES = {
    "string", "boolean", "bool", "int", "integer", "bigint", "long", "smallint",
    "tinyint", "double", "float", "decimal", "numeric", "date", "timestamp",
    "binary", "void", "array", "map", "struct",
}
IDENT = r"[A-Za-z_][\w$]*"


# ────────────────────────────────────────────────
# FABRIC / PATH HELPERS
# ────────────────────────────────────────────────
def _notebookutils():
    """Return Fabric notebookutils, or None when running outside Fabric."""
    try:
        import notebookutils as nbu  # type: ignore

        return nbu
    except Exception:
        return None


def get_runtime_paths() -> Dict[str, str]:
    """Resolve workspace, YAML root, and output Delta paths from Fabric context."""
    nbu = _notebookutils()
    if nbu is None:
        raise RuntimeError(
            "notebookutils is not available. Run this notebook in Microsoft Fabric "
            "or inject paths manually."
        )

    metadata_lh = nbu.lakehouse.get(METADATA_LAKEHOUSE_NAME)
    observ_lh = nbu.lakehouse.get(OBSERVABILITY_LAKEHOUSE_NAME)
    workspace_id = metadata_lh["workspaceId"]
    workspace_name = nbu.runtime.context.get("currentWorkspaceName") or ""

    yaml_root = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{metadata_lh['id']}/{YAML_FILES_SUBPATH}"
    )
    tables_root = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{observ_lh['id']}/Tables/{OUTPUT_SCHEMA}"
    )
    return {
        "workspace_name": workspace_name,
        "workspace_id": workspace_id,
        "yaml_root": yaml_root,
        "pii_table_path": f"{tables_root}/{PII_TABLE_NAME}",
        "source_columns_table_path": f"{tables_root}/{SOURCE_COLUMNS_TABLE_NAME}",
    }


def relative_files_path(full_path: str) -> str:
    """Prefer the path after Files/ so stored values stay readable."""
    marker = "Files/"
    if marker in full_path:
        return full_path.split(marker, 1)[1]
    return full_path


def iter_yaml_files(root_dir: str):
    """Yield (full_path, filename) for every .yaml/.yml under an ABFSS root."""
    fs = fsspec.filesystem("abfss", **STORAGE_OPTIONS)
    for dirpath, _dirnames, filenames in fs.walk(root_dir, detail=False):
        dirpath = dirpath.rstrip("/")
        for filename in filenames:
            if not filename.lower().endswith((".yaml", ".yml")):
                continue
            yield fs, posixpath.join(dirpath, filename), filename


def load_yaml_document(fs, full_path: str) -> Optional[dict]:
    try:
        with fs.open(full_path, mode="rt", encoding="utf-8") as handle:
            content = yaml.safe_load(handle)
        if isinstance(content, dict):
            return content
        logger.warning("Skipping non-mapping YAML: %s", full_path)
        return None
    except yaml.YAMLError as exc:
        logger.warning("Invalid YAML → skipping %s: %s", full_path, exc)
        return None
    except Exception as exc:
        logger.error("Failed to read %s: %s - %s", full_path, type(exc).__name__, exc)
        return None


def extract_target(content: dict) -> Optional[Tuple[str, str, str]]:
    target = content.get("target", {})
    if not isinstance(target, dict):
        return None
    lakehouse = target.get("lakehouse")
    schema_name = target.get("schema")
    table = target.get("table")
    if not all([lakehouse, schema_name, table]):
        return None
    return str(lakehouse), str(schema_name), str(table)


# ────────────────────────────────────────────────
# PII COLLECTION
# ────────────────────────────────────────────────
def collect_pii_info_from_yaml_files(root_dir: str) -> List[Dict[str, Any]]:
    """
    Recursively find YAML files and extract target + pii_columns when present.
    """
    records: List[Dict[str, Any]] = []
    files_checked = 0

    logger.info("Scanning YAML files for PII declarations: %s", root_dir)
    for fs, full_path, filename in iter_yaml_files(root_dir):
        files_checked += 1
        content = load_yaml_document(fs, full_path)
        if not content:
            continue

        target = extract_target(content)
        if not target:
            continue
        lakehouse, schema_name, table = target

        pii_list = content.get("target", {}).get("pii_columns", [])
        if not isinstance(pii_list, list) or not pii_list:
            continue

        records.append(
            {
                "lakehouse": lakehouse,
                "schema": schema_name,
                "table": table,
                "pii_columns": [str(col_name) for col_name in pii_list],
                "yaml_file_path": relative_files_path(full_path),
                "yaml_file_name": filename,
            }
        )

    logger.info("PII scan complete. Files checked: %s | tables with PII: %s", files_checked, len(records))
    return records


# ────────────────────────────────────────────────
# SQL PARSING  (aligned to dim_adjuster.yaml)
# ────────────────────────────────────────────────
def strip_sql_noise(sql: str) -> str:
    """Remove comments and replace string literals so identifiers can be scanned."""
    cleaned = re.sub(r"--.*?$", " ", sql, flags=re.MULTILINE)
    cleaned = re.sub(r"/\*.*?\*/", " ", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"'(?:''|[^'])*'", "''", cleaned)
    cleaned = re.sub(r'"(?:""|[^"])*"', '""', cleaned)
    return cleaned


def extract_table_refs(sql: str) -> List[Dict[str, str]]:
    """
    Extract FROM / JOIN table references and aliases.

    Handles:
      FROM claimant c
      INNER JOIN employee e
      LEFT JOIN lookups l
      FROM claims_adjustor_stats          (alias defaults to table name)
    """
    cleaned = strip_sql_noise(sql)
    pattern = re.compile(
        rf"""(?ix)
        (?:^|\s)
        (?:
            from
          | (?:inner|left|right|full|cross)\s+(?:outer\s+)?join
          | join
        )
        \s+
        ((?:{IDENT}\.){{0,2}}{IDENT})
        (?:\s+(?:as\s+)?({IDENT}))?
        """
    )
    refs: List[Dict[str, str]] = []
    seen = set()
    for match in pattern.finditer(cleaned):
        raw_name = match.group(1)
        alias = match.group(2)
        parts = raw_name.split(".")
        table = parts[-1]
        if table.lower() in SQL_KEYWORDS:
            continue
        if alias and alias.lower() in SQL_KEYWORDS:
            alias = None
        resolved_alias = alias or table
        key = (resolved_alias.lower(), table.lower())
        if key in seen:
            continue
        seen.add(key)
        refs.append(
            {
                "raw": raw_name,
                "table": table,
                "alias": resolved_alias,
            }
        )
    return refs


def extract_qualified_columns(sql: str) -> List[Tuple[str, str]]:
    """Return unique (qualifier, column) pairs such as e.empcode."""
    cleaned = strip_sql_noise(sql)
    pattern = re.compile(rf"\b({IDENT})\.({IDENT})\b")
    columns: List[Tuple[str, str]] = []
    seen = set()
    for match in pattern.finditer(cleaned):
        qualifier, column = match.group(1), match.group(2)
        if qualifier.lower() in SQL_KEYWORDS:
            continue
        if column.lower() in SQL_KEYWORDS or column.lower() in SQL_TYPES:
            continue
        key = (qualifier.lower(), column.lower())
        if key in seen:
            continue
        seen.add(key)
        columns.append((qualifier, column))
    return columns


def extract_unqualified_select_columns(sql: str) -> List[str]:
    """
    For simple SELECT lists without table qualifiers, e.g.

        SELECT adjustorstatsid, claimantid, transactiontype
        FROM claims_adjustor_stats
    """
    cleaned = strip_sql_noise(sql)
    select_match = re.search(r"(?is)\bselect\b(.*?)\bfrom\b", cleaned)
    if not select_match:
        return []
    select_list = select_match.group(1)
    columns: List[str] = []
    seen = set()
    for raw_item in select_list.split(","):
        item = raw_item.strip()
        if not item or item == "*":
            continue
        as_match = re.search(rf"(?i)\bas\s+({IDENT})\s*$", item)
        if as_match:
            continue
        # skip expressions / functions
        if re.search(r"[\(\+\-\*/]", item):
            continue
        if "." in item:
            continue
        ident_match = re.fullmatch(IDENT, item)
        if not ident_match:
            continue
        name = ident_match.group(0)
        if name.lower() in SQL_KEYWORDS or name.lower() in seen:
            continue
        seen.add(name.lower())
        columns.append(name)
    return columns


def build_source_lookup(content: dict) -> Dict[str, Dict[str, str]]:
    """
    Map source.name and source.table (lowercased) to the declared source block.
    Also map query block names so intermediate SQL can be tagged as query_block.
    """
    lookup: Dict[str, Dict[str, str]] = {}
    for source in content.get("source", []) or []:
        if not isinstance(source, dict):
            continue
        payload = {
            "source_name": str(source.get("name") or ""),
            "source_lakehouse": str(source.get("lakehouse") or ""),
            "source_schema": str(source.get("schema") or ""),
            "source_table": str(source.get("table") or ""),
            "source_kind": "declared_source",
        }
        for key in (payload["source_name"], payload["source_table"]):
            if key:
                lookup[key.lower()] = payload

    for query in content.get("query", []) or []:
        if not isinstance(query, dict):
            continue
        name = str(query.get("name") or "")
        if name and name.lower() not in lookup:
            lookup[name.lower()] = {
                "source_name": name,
                "source_lakehouse": "",
                "source_schema": "",
                "source_table": name,
                "source_kind": "query_block",
            }
    return lookup


def resolve_source(lookup: Dict[str, Dict[str, str]], qualifier: str, table_name: str) -> Dict[str, str]:
    for key in (qualifier, table_name):
        if key and key.lower() in lookup:
            return lookup[key.lower()]
    return {
        "source_name": table_name or qualifier or "",
        "source_lakehouse": "",
        "source_schema": "",
        "source_table": table_name or qualifier or "",
        "source_kind": "unresolved",
    }


def parse_sql_source_columns(
    sql: str,
    source_lookup: Dict[str, Dict[str, str]],
) -> List[Dict[str, str]]:
    """
    Parse one query.sql block and return source-column rows.

    Resolution order:
      1. FROM/JOIN alias → declared source.name / source.table
      2. alias.column references in SELECT / JOIN / WHERE
      3. unqualified SELECT columns when the query has a single FROM table
    """
    table_refs = extract_table_refs(sql)
    alias_to_table = {ref["alias"].lower(): ref["table"] for ref in table_refs}
    table_to_alias = {ref["table"].lower(): ref["alias"] for ref in table_refs}

    rows: List[Dict[str, str]] = []
    seen = set()

    def add_row(qualifier: str, column_name: str, reference_style: str) -> None:
        table_name = alias_to_table.get(qualifier.lower(), qualifier)
        source_meta = resolve_source(source_lookup, qualifier, table_name)
        grain = (
            source_meta["source_kind"],
            source_meta["source_lakehouse"],
            source_meta["source_schema"],
            source_meta["source_table"],
            qualifier.lower(),
            column_name.lower(),
        )
        if grain in seen:
            return
        seen.add(grain)
        rows.append(
            {
                "query_source_alias": qualifier,
                "source_name": source_meta["source_name"],
                "source_lakehouse": source_meta["source_lakehouse"],
                "source_schema": source_meta["source_schema"],
                "source_table": source_meta["source_table"],
                "source_column": column_name,
                "source_kind": source_meta["source_kind"],
                "reference_style": reference_style,
            }
        )

    for qualifier, column_name in extract_qualified_columns(sql):
        add_row(qualifier, column_name, "qualified")

    unqualified = extract_unqualified_select_columns(sql)
    if unqualified and len(table_refs) == 1:
        only = table_refs[0]
        for column_name in unqualified:
            add_row(only["alias"], column_name, "unqualified_select")

    # Ensure FROM tables themselves are represented even if no columns parsed
    if not rows:
        for ref in table_refs:
            add_row(ref["alias"], "*", "table_only")

    return rows


def collect_sql_source_columns_from_yaml_files(root_dir: str) -> List[Dict[str, Any]]:
    """
    Recursively scan YAML files and extract source table columns from query[].sql.
    """
    records: List[Dict[str, Any]] = []
    files_checked = 0

    logger.info("Scanning YAML files for SQL source columns: %s", root_dir)
    for fs, full_path, filename in iter_yaml_files(root_dir):
        files_checked += 1
        content = load_yaml_document(fs, full_path)
        if not content:
            continue

        target = extract_target(content)
        if not target:
            continue
        lakehouse, schema_name, table = target

        queries = content.get("query", [])
        if not isinstance(queries, list):
            continue

        source_lookup = build_source_lookup(content)
        relative_path = relative_files_path(full_path)

        for query in queries:
            if not isinstance(query, dict):
                continue
            sql_text = query.get("sql")
            query_name = str(query.get("name") or "")
            if not sql_text or not isinstance(sql_text, str):
                continue

            try:
                parsed_rows = parse_sql_source_columns(sql_text, source_lookup)
            except Exception as exc:
                logger.error(
                    "SQL parse failed in %s query=%s: %s - %s",
                    full_path,
                    query_name,
                    type(exc).__name__,
                    exc,
                )
                continue

            for parsed in parsed_rows:
                records.append(
                    {
                        "target_lakehouse": lakehouse,
                        "target_schema": schema_name,
                        "target_table": table,
                        "query_name": query_name,
                        "yaml_file_path": relative_path,
                        "yaml_file_name": filename,
                        **parsed,
                    }
                )

    logger.info(
        "SQL scan complete. Files checked: %s | source-column rows: %s",
        files_checked,
        len(records),
    )
    return records


# ────────────────────────────────────────────────
# DELTA WRITE
# ────────────────────────────────────────────────
def merge_or_create_delta(df: DataFrame, table_path: str, merge_predicate: str) -> None:
    """Upsert into an existing Delta path, or create the table on first run."""
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        logger.info("Delta table exists → merge into %s", table_path)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_predicate)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception as exc:
        logger.info("Creating new Delta table at %s (%s)", table_path, type(exc).__name__)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(table_path)
        )


def write_pii_table(records: List[Dict[str, Any]], workspace_name: str, table_path: str) -> Optional[DataFrame]:
    if not records:
        logger.info("No YAML files with target.pii_columns found.")
        return None

    df = spark.createDataFrame(records)
    df_exploded = df.select(
        lit(workspace_name).alias("workspace"),
        col("lakehouse"),
        col("schema"),
        col("table"),
        explode("pii_columns").alias("pii_column"),
        col("yaml_file_path"),
        col("yaml_file_name"),
        current_timestamp().alias("loaded_timestamp"),
    )
    merge_or_create_delta(
        df_exploded,
        table_path,
        """
        target.workspace  = source.workspace  AND
        target.lakehouse  = source.lakehouse  AND
        target.schema     = source.schema     AND
        target.table      = source.table      AND
        target.pii_column = source.pii_column
        """,
    )
    logger.info("PII table write complete | rows: %s", df_exploded.count())
    return df_exploded


def write_source_columns_table(
    records: List[Dict[str, Any]], workspace_name: str, table_path: str
) -> Optional[DataFrame]:
    if not records:
        logger.info("No SQL source columns found in YAML query blocks.")
        return None

    df = spark.createDataFrame(records).select(
        lit(workspace_name).alias("workspace"),
        col("target_lakehouse"),
        col("target_schema"),
        col("target_table"),
        col("query_name"),
        col("query_source_alias"),
        col("source_name"),
        col("source_lakehouse"),
        col("source_schema"),
        col("source_table"),
        col("source_column"),
        col("source_kind"),
        col("reference_style"),
        col("yaml_file_path"),
        col("yaml_file_name"),
        current_timestamp().alias("loaded_timestamp"),
    )
    merge_or_create_delta(
        df,
        table_path,
        """
        target.workspace           = source.workspace           AND
        target.target_lakehouse    = source.target_lakehouse    AND
        target.target_schema       = source.target_schema       AND
        target.target_table        = source.target_table        AND
        target.query_name          = source.query_name          AND
        target.query_source_alias  = source.query_source_alias  AND
        target.source_column       = source.source_column
        """,
    )
    logger.info("SQL source-column table write complete | rows: %s", df.count())
    return df


# ────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────
def main() -> None:
    start = time.time()
    paths = get_runtime_paths()

    logger.info("Workspace: %s", paths["workspace_name"])
    logger.info("YAML root: %s", paths["yaml_root"])
    logger.info("PII table: %s", paths["pii_table_path"])
    logger.info("Source-column table: %s", paths["source_columns_table_path"])

    pii_records = collect_pii_info_from_yaml_files(paths["yaml_root"])
    write_pii_table(pii_records, paths["workspace_name"], paths["pii_table_path"])

    sql_records = collect_sql_source_columns_from_yaml_files(paths["yaml_root"])
    write_source_columns_table(
        sql_records, paths["workspace_name"], paths["source_columns_table_path"]
    )

    logger.info("Job completed in %.1f seconds", time.time() - start)


if __name__ == "__main__":
    main()
