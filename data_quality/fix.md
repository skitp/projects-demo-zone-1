**Fixed.** The Great Expectations 1.24.0 regression is resolved in `data_quality.py`.

### Root Cause
GE 1.24.0 changed `unexpected_index_query` for Spark from a raw fragment into a valid Python expression whose condition is a **quoted SQL string**:

```python
df.filter(F.expr("NOT (`col` IS NOT NULL)"))
```

The old code did:

```python
expectation_filter = self._fix_gx_filter(unexpected_index_query[17:-2])
when(expr(expectation_filter), ...)
```

After 1.24.0 the slice often left the surrounding quotes, so `expr()` produced a **STRING** column. Spark then rejected the `CASE WHEN` with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`.

### Changes Made
1. Added `_extract_sql_condition()` — robustly unwraps both the old and new GE formats, strips wrappers (`df.filter(`, `F.expr(`), removes outer quotes, and cleans residual artifacts.
2. Replaced the fragile `_fix_gx_filter()` with a more defensive version that correctly normalizes `IN` lists and `RLIKE`/`REGEXP` patterns.
3. Updated the `perform_expectations` path to use the new extractor so the condition passed to `when(expr(...))` is always a proper boolean SQL expression.
4. Added debug/warning logging for the extracted filter.

The fixed file is ready:




You can drop this in place of the previous `data_quality.py`. The rest of the DQ pipeline (quarantine, lookup expectations, logging, etc.) is unchanged.
