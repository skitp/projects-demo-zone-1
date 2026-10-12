This is the root cause. SQL Server CDC emits two rows for an update:

- `__$operation = 3` — before-image (old primary-key values)
- `__$operation = 4` — after-image (new primary-key values)

Your incremental query keeps only operations 1, 2 and 4 and partitions by the key columns. When a primary-key column itself is updated:

1. The before-image (op 3) is discarded.
2. The after-image arrives with the **new** key and is treated as an insert (or unmatched update).
3. The merge inserts a new active row for the new key.
4. The old key in the target is never matched, so it stays `dl_iscurrent = 1`.
5. Active row count in the target increases by one; the business source has only the new key.

No pure key-based merge can correct this without also consuming the before-image. Processing op 3 to soft-delete the old key (then applying op 4 as an insert) is possible but fragile, requires strict ordering by LSN + command_id, and still breaks if the before-image falls outside the capture window or is cleaned up. Given that the source application permits primary-key updates, there is no reliable fix inside the current merge design.

---

**RCA: Active Row Count Drift on CDC Incremental Loads**

**ID:** CDC-PK-UPDATE-001  
**Status:** Closed – By Design Limitation  
**Severity:** High (data correctness)  
**Date:** 2026-10-12  
**Author:** Data Engineering  

**Summary**  
After a full load, incremental CDC loads produce a higher active row count in the target than in the source. The excess rows are former primary-key values that were updated in the source system.

**Impact**  
- Target active count (`dl_isdeleted = 0` / `dl_iscurrent = 1`) diverges from the source.  
- Downstream consumers see duplicate or stale business keys.  
- Reconciliation and data-quality checks fail.

**Detection**  
- Full load matched.  
- Post-incremental active count in target > source.  
- Diagnostic table `quarantine.<table>_unmatched` contained keys with `__operation = 2` or `4`.  
- Those keys existed in the CDC change table but not in the current business source.  
- Manual inspection of CDC showed op 3 (before) and op 4 (after) with different primary-key values.

**Root Cause**  
The source application allows updates to columns that form the primary key.  
SQL Server CDC records such an update as:  
- op 3 = before-image (old key)  
- op 4 = after-image (new key)  

The incremental extract filters to operations `(1, 2, 4)` and takes the latest row per key. The before-image is dropped. The merge therefore:  
- inserts the new key (unmatched), and  
- leaves the old key untouched and still active.

**Contributing Factors**  
- CDC extract excludes operation 3.  
- Merge logic is purely key-based and has no concept of “key rename.”  
- No reconciliation against a current source snapshot.

**Resolution**  
No change to the merge will make key-renames safe. Accepted options:  
1. Stop updating primary-key columns in the source application (preferred).  
2. Add a periodic reconciliation job that soft-deletes target keys absent from a current source snapshot.  
3. (High effort / still imperfect) Process op 3 to soft-delete the old key before applying op 4; requires guaranteed LSN ordering and retention of before-images.

**Preventive Actions**  
- Document that primary-key updates are unsupported by the CDC merge path.  
- Add a data-quality check that alerts when active target count exceeds source count after an incremental load.  
- Retain the unmatched-key diagnostic for ongoing monitoring.

**Lessons Learned**  
CDC incremental merges that rely solely on the after-image cannot handle primary-key mutations. Either the source must treat keys as immutable, or the pipeline must reconcile against a full current snapshot.
