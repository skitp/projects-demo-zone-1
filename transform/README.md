# Cross-Workspace Lakehouse Support

## Overview

The Transform engine (`transform.py`) and the orchestration notebook (`notebook.py`) have been refactored to support **cross-workspace lakehouses** in Microsoft Fabric.

Previously, all lakehouse references in YAML configs assumed the lakehouse existed in the current workspace (or was available via an attached shortcut / `notebookutils.lakehouse.get()`). The updated code allows sources and the target table to live in a different workspace by supplying explicit `workspace_id` and `lakehouse_id`.

This is useful when:
- The product notebook runs in a control / orchestration workspace
- Source curated tables live in a domain or source-system workspace
- The final dimensional / fact tables are written to a product workspace
- You want to avoid creating OneLake shortcuts for every cross-workspace dependency

## YAML Configuration Changes

### Target

```yaml
target:
  lakehouse: den_lhw_dpr_001_claims_transaction
  # Optional – supply both for cross-workspace target
  workspace_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  lakehouse_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  schema: claims_transaction
  table: dim_cat
  load_strategy: type_two
  ...
```

### Sources

```yaml
source:
  - name: curated_claims_catcodes
    lakehouse: den_lhw_scu_001_claims_transaction_curated
    # Optional – supply both for cross-workspace source
    workspace_id: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
    lakehouse_id: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
    schema: iis_transaction
    table: claims_catcodes
```

**Rules**
- If **both** `workspace_id` and `lakehouse_id` are present → the engine builds a direct `abfss://` path and does **not** call `LakehouseManager` / `notebookutils` for that object.
- If either is missing → classic same-workspace behavior (LakehouseManager by name).
- You can mix same-workspace and cross-workspace sources in a single YAML.
- `lakehouse` name is still required (used for metrics, logging, and as a human-readable label).

## Implementation Details

### `transform.py`

- New internal class `CrossWorkspaceLakehouse` provides a minimal compatible surface:
  - `build_delta_file_path(table, schema)`
  - `write_delta_table(...)` (used by the PII redaction clone path)
- `Transform.__init__` inspects the target block and instantiates either `CrossWorkspaceLakehouse` or the normal `LakehouseManager`.
- New helper `_build_source_path(source)` constructs the correct Delta path for each source.
- `_generate_source_views` now uses the helper (no longer assumes a single LakehouseManager per lakehouse name).

### Compatibility with Load Strategies

`LoadTypes.configure_load` and `.transform` receive the `target_lakehouse` object.  
- Same-workspace → real `LakehouseManager` (full feature set).
- Cross-workspace → `CrossWorkspaceLakehouse` (path-based reads/writes).  
  Most load strategies that ultimately call `df.write...save(path)` or use the manager’s `build_delta_file_path` continue to work. Strategies that rely on other LakehouseManager methods (e.g. schema evolution helpers, custom catalog registration) may need a small adapter if you hit them.

### `notebook.py`

No functional changes were required. The notebook already supports explicit `workspace_id` / lakehouse IDs for the metadata and observability lakehouses (used for feed JSON, YAML configs, and run logs). The transform configs it loads now inherit the cross-workspace capability automatically.

## Permissions & Prerequisites

1. The identity running the notebook (user or Service Principal) must have **at least Read** access on the source workspace/lakehouse and **Write** access on the target workspace/lakehouse.
2. In Fabric, cross-workspace OneLake access works via the full `abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/...` path when the caller has the appropriate role (Contributor / Member / Admin or the equivalent item-level permission).
3. You do **not** need to attach the remote lakehouse to the notebook when using the ID-based path.
4. Incremental sources still look up the observability log table via the standard `LakehouseManager` (same-workspace). If the observability lakehouse itself is cross-workspace, that path can be extended the same way later.

## Example: dim_cat

See `dim_cat.yaml` in this folder. The optional `workspace_id` / `lakehouse_id` keys are commented out so the file remains valid for same-workspace runs. Uncomment and fill in the GUIDs when you need cross-workspace behavior.

## Migration Checklist

- [ ] Identify which sources or targets live in another workspace.
- [ ] Obtain the Workspace ID and Lakehouse ID (from the Fabric portal URL or `notebookutils.lakehouse.get` in the source workspace).
- [ ] Add the two optional keys to the corresponding YAML blocks.
- [ ] Confirm the running identity has access.
- [ ] Test a single transform (e.g. `dim_cat`) before enabling the full feed.
- [ ] Monitor the first run’s metrics / quarantine / PII clone paths.

## Limitations & Future Work

- The observability / incremental watermark path still uses the classic `LakehouseManager` by name. Cross-workspace support for that lakehouse can be added the same way if needed.
- Advanced LakehouseManager features (beyond path construction and basic Delta write) are not replicated in `CrossWorkspaceLakehouse`. If a specific load strategy fails, capture the missing method and we can extend the adapter.
- Schema-on-read / column mapping / liquid clustering settings are inherited from the Delta log; no special handling is required.

## Files Delivered

| File | Purpose |
|------|---------|
| `transform.py` | Updated Transform class with cross-workspace path resolution and adapter |
| `notebook.py` | Orchestration notebook (compatible, no breaking changes) |
| `dim_cat.yaml` | Example config showing optional cross-workspace keys |
| `README.md` | This document |

---
*Updated 2026-11-19*
