**Design Decision: Unify CDC MERGE into a Single Path**

**Proposal**  
Always ingest operations 1, 2, 3 and 4.  
One merge implementation for every CDC table:
- Latest op 1 or 3 per key → soft-delete  
- Latest op 2 or 4 per key → upsert (reactivate if needed)  

Remove the `mutable_candidate_keys` flag and the dual-path branch.

### Pros
- **Correct for every table** — PK updates (op 3 + 4) are handled even if a table was previously assumed immutable. No silent drift when the source application later allows key changes.
- **Simpler CDC contract** — Extract query is identical for all tables (`IN (1,2,3,4)` + latest-per-key). No per-table query variants or configuration flags to keep in sync.
- **Less code and lower maintenance** — Single code path, one set of tests, no risk of the two branches diverging over time.
- **Operational clarity** — Logging, unmatched-key diagnostics, and metrics behave the same way for every CDC load.
- **Future-proof** — New tables or schema changes that introduce key mutability require zero pipeline changes.

### Cons
- **Slightly more work on immutable tables** — Op 3 rows are read and evaluated even though they should be rare or absent. Negligible at typical CDC volumes, but not zero.
- **Behavior change for existing immutable tables** — If any op 3 rows exist today they will now soft-delete the old key (previously ignored). This is usually desirable, but it is a behavioral change that must be validated.
- **Ordering assumption is stronger** — The unified path relies on “latest operation per key wins.” True intra-batch sequencing anomalies (extremely rare with proper LSN ordering) are handled identically to the mutable path; there is no longer a simpler fallback.
- **Requires coordinated extract change** — All CDC queries must be updated to include op 3 before the unified merge is deployed. Partial rollout would drop before-images and re-introduce the original drift.
- **Marginally harder mental model for simple tables** — Engineers working only with immutable keys must understand why op 3 appears in the stream.

### Recommendation
**Adopt the unified path.**

The correctness and simplicity benefits outweigh the minor extra processing and the one-time extract change. The original drift bug is eliminated for all tables without relying on a configuration flag that can be set incorrectly. Validate on a representative immutable table (confirm op 3 volume is near zero and active counts remain stable) before full rollout.
