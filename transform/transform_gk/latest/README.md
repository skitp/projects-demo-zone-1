# Cross-Workspace Lakehouse Support (Simplified)

## Overview

A minimal change that lets the Transform engine resolve source and target lakehouses in a workspace other than the one where the notebook is running.

**Constraints of this design**
- No changes to any YAML files.
- One `store_workspace_id` is supplied by the notebook and must be set before `start_transform`.
- Every source lakehouse and the target lakehouse named in the YAML are assumed to live in that same workspace.
- The observability lakehouse always resolves in the workspace where the code is executing.
- Path resolution for YAML lakehouses is done exclusively through the existing `LakehouseManager` class.

## How it works

1. The notebook already holds a `workspace_id` variable.
2. After `configure_transform(...)` and before `start_transform(...)` the notebook calls:

   ```python
   .set_store_workspace_id(workspace_id)
   ```

3. `Transform.set_store_workspace_id` stores the value and recreates the target `LakehouseManager` with it.
4. When source views are generated, every source `LakehouseManager` is also constructed with the same `store_workspace_id`.
5. The observability lakehouse is created **without** the store workspace ID so it resolves in the executing workspace.
6. `LakehouseManager` builds the correct `abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/...` path for YAML lakehouses.

If `set_store_workspace_id` is never called, behaviour is identical to the previous same-workspace implementation.

## Notebook change

In `process_data` the call chain becomes:

```python
data = (
    SparkEngine.transform(product_config_path)
    .configure_transform(
        product_name=product_name,
        feed_name=feed_name,
        dataset_name=file_name
    )
    .set_store_workspace_id(workspace_id)    # <-- new
    .start_transform(elt_id=elt_id, run_id=run_id)
    .metrics()
)
```

`workspace_id` is the variable already defined at the top of the notebook.

## Transform changes

- New attribute `self.store_workspace_id` (default `None`).
- New method `set_store_workspace_id(store_workspace_id: str) -> self` (chainable).
- `_init_target_lakehouse()` creates the target `LakehouseManager` and `target_path`, using the current `store_workspace_id`.
- `_generate_source_views` passes `workspace_id=self.store_workspace_id` only when constructing source `LakehouseManager` instances.
- Observability `LakehouseManager` is created without a workspace ID override.

No other logic was modified.

## LakehouseManager

The class already supports the required constructor signature:

```python
LakehouseManager(lakehouse_name: str, workspace_id: Optional[str] = None)
```

When a GUID is supplied it is used directly; otherwise `notebookutils.lakehouse.get` is used to resolve the name to an ID (and the workspace if none was provided).

## What is not covered

- Different workspaces for different sources or for the target (all YAML lakehouses share the single `store_workspace_id`).
- Any YAML-level token replacement or per-object IDs.

## Files

| File | Change |
|------|--------|
| `transform.py` | Added `set_store_workspace_id`; applied only to YAML sources and target |
| `notebook.py` | Calls `.set_store_workspace_id(workspace_id)` before `start_transform` |
| `lakehouse.py` | Unchanged (already accepts `workspace_id`) |
| `README.md` | This document |

## Error fix (ArtifactNotFoundException)

`notebookutils.lakehouse.get(name)` only sees lakehouses in the current workspace. When `store_workspace_id` points to a different workspace the call fails with:

```
ArtifactNotFoundException: Artifact 'den_lhw_dpr_001_data_product_tables' not found
```

**Resolution**

When `store_workspace_id` is set, Transform now:

1. Calls the Fabric REST API (`GET /v1/workspaces/{id}/lakehouses`) using a token from `notebookutils.credentials.getToken`.
2. Maps each YAML lakehouse name to its ID in that workspace (result is cached).
3. Passes the **GUID** to `LakehouseManager` together with the store workspace ID.

`LakehouseManager` then treats the value as an ID and does not call `notebookutils.lakehouse.get`. Observability lakehouse continues to resolve in the executing workspace (no override).

The identity running the notebook must be able to list lakehouses in the store workspace.

## Logging

Standard Python logging (`logger = logging.getLogger(__name__)`) is used throughout Transform:

- INFO when `store_workspace_id` is set and when lakehouse names are resolved
- INFO for the target path and source lakehouse resolution
- INFO at the start and successful end of `start_transform` (with insert/update/delete counts)
- ERROR (with stack trace) if the transform fails

## Important: deferred initialization

`SparkEngine.transform(config_path)` constructs the `Transform` object. Lakehouse resolution **must not** happen inside `__init__`, because `set_store_workspace_id` is called later in the chain. The target `LakehouseManager` is created only inside `set_store_workspace_id` (or lazily at the start of `start_transform` for same-workspace runs). This prevents the `ArtifactNotFoundException` from being raised before the store workspace ID is known.

## Latest error (after name resolution)

After the lakehouse name was successfully resolved, the next failure was inside `Expectations.__init__`:

```python
self.lakehouse = LakehouseManager(lakehouse_name)   # no workspace_id
```

That again called `notebookutils.lakehouse.get` with the name and raised `ArtifactNotFoundException`.

**Fix**

- `Expectations` now accepts optional `workspace_id` and `lakehouse_manager`.
- When a pre-resolved manager is supplied it is used directly.
- Transform passes `lakehouse_manager=self.target_lakehouse` (the manager already created with the store workspace ID).
- The DQ / observability lakehouse inside Expectations is still created without a workspace override (executing workspace).

Deploy the updated `data_quality.py` (Expectations) together with the updated `transform.py`.

---
*Updated 2026-11-25*
