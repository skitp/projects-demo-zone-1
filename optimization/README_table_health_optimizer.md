# Delta Table Health Metrics & Cost-Aware Auto-OPTIMIZE

Production-ready Fabric notebook that implements a **config-driven, cost-aware** equivalent of `sys.sp_get_table_health_metrics` for Delta Lake tables.

## Files

| File | Purpose |
|------|---------|
| `delta_table_health_optimizer_notebook.py` | Main notebook (copy into a Fabric notebook or import as source) |
| `table_health_config.json` | Scoring rules, recommendation thresholds and optimization gates |

## Key Design Principles

1. **Score is a cost/benefit signal**, not a binary flag.  
   Higher score = more urgency to OPTIMIZE.

2. **Auto-OPTIMIZE is heavily gated**:
   - Table must be ≥ `minimum_table_size_gb` (default 5 GB)
   - Must have ≥ `minimum_file_count` (default 1 000)
   - Average file size must be ≤ `maximum_average_file_size_mb` (default 128 MB)
   - Health score must reach `auto_optimize_threshold` (default 75)
   - Global switch `allow_auto_optimize` (default **false**)

3. **Safe defaults**: `dry_run = true` and `allow_auto_optimize = false`.

4. **Daily idempotent upsert** into `housekeeping.table_health_metrics` so you always have the latest assessment per table per day.

## How Scoring Works

```
score = small_file_score + file_count_score + optimize_age_score + dml_activity_score
```

| Component              | High score when…                          |
|------------------------|-------------------------------------------|
| Small-file pressure    | avg file size ≤ 32 / 64 / 128 MB          |
| File count             | > 1 k / 5 k / 10 k files                  |
| Optimize age           | last OPTIMIZE was > 7 / 14 / 30 days ago  |
| Recent DML             | many MERGE/UPDATE/DELETE in last 14 days|

**Recommendation mapping**
- `score < 20` → `HEALTHY`
- `20–39` → `MONITOR`
- `40–74` → `OPTIMIZE_SOON`
- `≥ 75` → `OPTIMIZE_NOW`

## Deployment in Microsoft Fabric

1. Upload `table_health_config.json` to the lakehouse **Files** area (or any path you prefer).
2. Create a new Fabric notebook and paste the contents of `delta_table_health_optimizer_notebook.py` (or upload the `.py` and import it).
3. Set notebook parameters (or edit the defaults):
   - `config_path` – location of the JSON
   - `target_schemas` – comma-separated list of schemas to scan
   - `table_filter` – optional table-name filter
   - `dry_run` – leave `true` for the first few runs
   - `force_optimize` – only set to `true` when you intentionally want to override the config switch
4. Schedule the notebook (daily or weekly) via a Fabric Pipeline or Spark job definition.
5. Monitor results with:

```sql
SELECT *
FROM housekeeping.table_health_metrics
WHERE assessment_date >= current_date() - 7
ORDER BY health_score DESC;
```

## Enabling Auto-OPTIMIZE

In `table_health_config.json`:

```json
"optimization_rules": {
  ...
  "allow_auto_optimize": true
}
```

Or override at runtime with the `force_optimize=true` parameter (use sparingly).

## Extending the Rules

All thresholds live in the JSON. You can change scores, add new severity bands, or tighten the size/file-count gates without touching the notebook code.

## Safety Notes

- OPTIMIZE is **never** run when `dry_run=true`.
- OPTIMIZE is **never** run when the table fails any of the four gates (size, file count, avg size, score).
- Each table is assessed and upserted independently – a failure on one table does not stop the rest.
- History look-back is limited (`history_limit`) to keep the notebook fast on large tables.
