# Durable Identity Key Map

**Audience:** Domain Engineers, Data Platform, Power BI consumers  
**Owner:** Platform Services / Domain Engineering  
**Last updated:** 2026-11-12  
**Status:** Active  
**Related code:** `strategy_durable_identity.py`, `seed_identity_map.py`

---

## 1. Purpose

Keep surrogate keys stable when a dimension (or any keyed table) is reloaded — overwrite, drop/recreate, or full rebuild — so fact tables and the Power BI semantic model continue to join on the same `bigint` without a model change or fact reload.

---

## 2. Problem

The previous `add_identity` minted keys with `max(identity) + row_number()` on the table itself. That number is not durable:

- `OverwriteLoad` or a table rebuild remints sequential keys.
- Existing fact foreign keys become orphaned.
- Power BI relationships stay type-correct (`bigint`) but point at the wrong members.

A separate, persistent map is required so the source table can be treated as disposable content while the published surrogate remains stable.

---

## 3. Solution

A per-table Delta map, independent of the source table:

```text
{schema}.map_{table}_identity
```

Two keys, two jobs:

| Key | Type | Role |
|-----|------|------|
| `bk_hash` | SHA2-256 string | Internal lookup of the natural / business key. Never published to the semantic model. |
| `{identity_col}` | sequential `bigint` | Published surrogate on the source table and facts. Dense integer for Power BI. |

Identity is minted **once** per distinct business key and never updated. The map, not the source table, is the source of truth.

### Map table schema

Example for `claims_transaction.dim_adjuster` → `claims_transaction.map_dim_adjuster_identity`:

| Column | Type | Purpose |
|--------|------|---------|
| `bk_hash` | string | Merge key (SHA2-256 of normalized candidate keys) |
| `<candidate keys>` | * | Original business key columns for debug and backfill |
| `<identity_col>` | bigint | Durable surrogate (`> 0`). Example: `adjustr_key` |
| `dl_map_insert_ts` | timestamp | When the identity was minted |
| `dl_elt_id` / `dl_run_id` | string | Lineage of the minting run |

### Frozen hash recipe

Do not change this after go-live. A recipe change remints keys and orphans facts.

```text
sha2(concat_ws('||', coalesce(cast(k as string), '')), 256)
```

### Runtime flow (`resolve`)

1. Select distinct candidate keys from the incoming batch.
2. Compute `bk_hash`.
3. Left-anti join against the map to find only unseen business keys.
4. Mint new sequential bigints from `max(map.identity)` using `row_number()` on the miss set only.
5. `MERGE` into the map on `bk_hash` with `WHEN NOT MATCHED INSERT` (never update an existing identity).
6. Join the map back to the full incoming DataFrame and drop the hash. Return the bigint identity first.

### Reserved values

Unknown members stay on the source table only (`add_unknown` runs after identity):

- `-1` = unknown
- `-2` = not applicable

These values are never minted by the map.

### Load strategy behavior

| Strategy | Behavior |
|----------|----------|
| MergeLoad (SCD1) | Matching business key reuses the same bigint. Attribute updates do not change the surrogate. |
| TypeTwoLoad (SCD2) | All versions of one business key share one surrogate. History is carried by dates and `dl_is_current_flag`. |
| OverwriteLoad | Table rewrite re-joins the map. Same surrogate, new attributes. Facts stay valid. |
| OverwriteByKey | Same resolve, then delete+append on the business key. Surrogate still comes from the map. |

SCD2 with fact stability requires a **shared** surrogate per business key. A new surrogate per version would force a fact reload or a business-key join in Power BI.

---

## 4. Power BI and semantic model

- No model change. Relationships remain `fact.{key}` → `dim.{key}` (`bigint`).
- `bk_hash` is never exposed to the dataset.
- Dense sequential integers preserve VertiPaq compression. No string-hash or sparse 64-bit keys.
- Existing consumers continue to work after a source-table overwrite.

---

## 5. Seed / backfill (required before cutover)

For any table that already has surrogate values in facts, **seed the map before** the first production load that uses the new `add_identity`. Until the map is seeded, the first resolve would mint a new sequence and break existing facts.

The utility is generic: any Delta table with candidate keys and a numeric identity column. `dry_run=True` is the default.

### What the seed does

- Computes `bk_hash` with the frozen recipe above.
- Collapses to one identity per candidate-key combination.
- Prefers the current version when `dl_is_current_flag` is present and true; otherwise takes the minimum positive identity.
- Excludes reserved values (`-1`, `-2`) unless you override `reserved_values`.
- Insert-only merge on `bk_hash` — never overwrites an existing map identity.
- Returns counts so you can review before writing.

### Path-based usage

```python
from seed_identity_map import seed_identity_map

# 1. Dry run
summary = seed_identity_map(
    spark=spark,
    source_path=lh.build_delta_file_path("dim_adjuster", "claims_transaction"),
    map_path=lh.build_delta_file_path("map_dim_adjuster_identity", "claims_transaction"),
    candidate_keys=["adjustr_bus_key"],
    identity_col="adjustr_key",
    elt_id="seed-dim-adjuster",
    dry_run=True,
)
print(summary.as_dict())

# 2. Write only after counts look right
summary = seed_identity_map(
    spark=spark,
    source_path=lh.build_delta_file_path("dim_adjuster", "claims_transaction"),
    map_path=lh.build_delta_file_path("map_dim_adjuster_identity", "claims_transaction"),
    candidate_keys=["adjustr_bus_key"],
    identity_col="adjustr_key",
    elt_id="seed-dim-adjuster",
    dry_run=False,
)
print(summary.as_dict())
```

### Lakehouse wrapper

```python
from seed_identity_map import seed_identity_map_via_lakehouse

summary = seed_identity_map_via_lakehouse(
    spark=spark,
    lakehouse=lh,
    schema="claims_transaction",
    source_table="dim_adjuster",
    candidate_keys=["adjustr_bus_key"],
    identity_col="adjustr_key",
    elt_id="seed-dim-adjuster",
    dry_run=True,
)
print(summary.as_dict())
```

Map name defaults to `map_{source_table}_identity` in the same schema. Override with `map_table` / `map_schema` if needed.

### Counts to review on the dry run

| Field | Meaning |
|-------|---------|
| `source_distinct_keys` | Distinct candidate-key combinations on the source |
| `source_positive_sk_rows` | Distinct keys with a positive identity |
| `reserved_excluded` | Rows with `-1` / `-2` (not seeded) |
| `conflicts` | Keys that currently have more than one positive SK |
| `preferred_current_used` | How many chosen rows came from the current flag |
| `rows_to_seed` | New map rows that would be inserted |
| `already_in_map` | Keys already present (left untouched) |
| `max_identity_after` | Max identity on the map after the write (or current max on dry run if the map exists) |

Investigate `conflicts` before writing. The seed still picks one identity (current preferred, else min), but a high conflict count usually means dirty history or an unexpected multi-version pattern.

### Cutover order

1. Dry-run the seed. Review counts, especially `conflicts` and `rows_to_seed`.
2. Seed the map (`dry_run=False`).
3. Deploy the patched strategy (`IdentityKeyMap` + `add_identity`).
4. Subsequent overwrite / merge / type-two loads only insert new hashes.

---

## 6. Operations

- Treat `map_*_identity` as a **control table**: no overwrite and no drop as part of a source-table refresh.
- Co-locate the map in the same schema as the source table so OneLake security stays aligned. See the Platform Services guidelines for rewrite and role re-application.
- `row_number()` runs only on the miss set (unpartitioned window). First load of a large distinct-key set is the only expensive mint; later runs are cheap.
- Serialize first-load or large-mint jobs. Concurrent pipelines that both compute `max + row_number` before either merge lands can race. The insert-only merge keeps one winner; the loser must not assign a second key.
- Hash collisions on SHA2-256 of these business keys are not a practical concern. Changing the stringification recipe is the real risk.

---

## 7. Estimation (ADO style)

| ID | Task | Hours |
|----|------|------:|
| T01 | Design review and freeze hash recipe | 4 |
| T02 | Implement IdentityKeyMap | 8 |
| T03 | Integrate into LoadStrategy | 4 |
| T04 | Validate load strategies | 8 |
| T05 | Backfill / seed utility | 6 |
| T06 | Pilot on dim_adjuster | 4 |
| T07 | README and runbook | 3 |
| T08 | Code review and PR | 3 |
| T09 | UAT with Power BI consumers | 4 |
| T10 | Buffer / edge cases | 4 |
| | **Total** | **48** |

Suggested work-item structure: Epic → Feature (48 h) → Tasks T01–T10. Critical path is T01 → T02 → T03 → T04 → T05 → T06.

---

## 8. References

- `artifacts/strategy_durable_identity.py` — `IdentityKeyMap` class and helpers
- `artifacts/seed_identity_map.py` — generic seed / backfill utility
- `artifacts/Durable_Identity_Key_Map_Solution_Design.docx` — full solution design
- Platform Services Guidelines for Domain Engineers — table rewrite and OneLake security
