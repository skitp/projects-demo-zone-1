# RCA: Active Row Count Drift on CDC Incremental Loads

**ID:** CDC-PK-UPDATE-001  
**Status:** Closed – By Design Limitation  
**Severity:** High  
**Date:** 2026-10-12  

## Summary
Incremental CDC loads leave more active rows in the target than exist in the source.

## Root Cause
The source application allows updates to primary-key columns. SQL Server CDC emits:

- op 3 = before-image (old key)
- op 4 = after-image (new key)

The extract keeps only operations 1/2/4, so the old key is never soft-deleted. The merge inserts the new key and leaves the old key active.

## Impact
Target active count drifts upward; stale keys remain visible to consumers.

## Resolution
No reliable fix inside the current key-based merge.

- **Preferred:** make primary keys immutable in the source.
- **Alternative:** periodic reconciliation that soft-deletes target keys missing from a current source snapshot.

## Prevention
Document the limitation and alert when post-incremental active target count exceeds source count.
