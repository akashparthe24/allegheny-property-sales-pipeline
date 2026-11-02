"""Data quality checks: post-load (run after property_sales_load is loaded)."""
from . import config


# One or more post-load checks failed; holds every failure, not just the first.
class CheckFailure(RuntimeError):
    def __init__(self, failures):
        self.failures = failures
        detail = "; ".join(f"{f['name']}: {f['detail']}" for f in failures)
        super().__init__(f"{len(failures)} check(s) failed: {detail}")


# Should always pass -- the matching unique index makes duplicates impossible.
def _check_no_duplicate_key(con, name, key_columns, index_where):
    cols = ", ".join(f'"{c}"' for c in key_columns)
    n = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT {cols}, COUNT(*) c
            FROM {config.LOAD_TABLE}
            WHERE {index_where}
            GROUP BY {cols}
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    return {"name": name, "passed": n == 0,
            "detail": f"{n} duplicate key group(s) found" if n else "ok"}


# Fails if `column` is blank (NULL or '') on any row.
def _check_not_null(con, column):
    n = con.execute(
        f'SELECT COUNT(*) FROM {config.LOAD_TABLE} WHERE "{column}" IS NULL OR "{column}" = \'\''
    ).fetchone()[0]
    return {"name": f"{column.lower()}_not_null", "passed": n == 0,
            "detail": f"{n} row(s) with blank {column}" if n else "ok"}


# A negative price is nonsensical and would distort any median computed from it.
def _check_price_not_negative(con):
    n = con.execute(
        f'SELECT COUNT(*) FROM {config.LOAD_TABLE} '
        f'WHERE "PRICE" IS NOT NULL AND "PRICE" != \'\' AND CAST("PRICE" AS REAL) < 0'
    ).fetchone()[0]
    return {"name": "price_not_negative", "passed": n == 0,
            "detail": f"{n} row(s) with a negative PRICE" if n else "ok"}


# Dataset-specific check: RECORDDATE before SALEDATE is a logical impossibility
# (e.g. a typo'd year). Allows a small baseline rate (~0.14%, real clerical noise).
def _check_recorded_before_sale_rate(con):
    row = con.execute(
        f"""
        SELECT
            SUM(CASE WHEN "RECORDDATE" < "SALEDATE" THEN 1 ELSE 0 END) AS bad,
            COUNT(*) AS total_with_both
        FROM {config.LOAD_TABLE}
        WHERE "RECORDDATE" != '' AND "SALEDATE" != ''
        """
    ).fetchone()
    bad, total_with_both = row[0] or 0, row[1] or 0
    pct = (100.0 * bad / total_with_both) if total_with_both else 0.0
    threshold = config.MAX_RECORDED_BEFORE_SALE_PCT
    return {
        "name": "recorded_before_sale_rate",
        "passed": pct <= threshold,
        "detail": (
            f"{bad}/{total_with_both} ({pct:.3f}%) rows have RECORDDATE before SALEDATE, "
            f"vs. the {threshold}% allowed. A small nonzero rate is expected (clerical typos, "
            "e.g. a year entered as '0212' instead of '2012'); a rate this high suggests a "
            "date-parsing or source problem, not routine clerical noise."
        ),
    }


# UPDATE_TIMESTAMP should never be earlier than INSERT_TIMESTAMP.
def _check_timestamp_consistency(con):
    n = con.execute(
        f'SELECT COUNT(*) FROM {config.LOAD_TABLE} WHERE "UPDATE_TIMESTAMP" < "INSERT_TIMESTAMP"'
    ).fetchone()[0]
    return {"name": "timestamp_consistency", "passed": n == 0,
            "detail": f"{n} row(s) where UPDATE_TIMESTAMP precedes INSERT_TIMESTAMP (upsert bug)" if n else "ok"}


# Run every post-load check; raise CheckFailure (with all failures) if any failed.
def run_post_load_checks(con):
    result = [
        _check_no_duplicate_key(con, "no_duplicate_main_key", config.MAIN_KEY, config.MAIN_INDEX_WHERE),
        _check_no_duplicate_key(con, "no_duplicate_fallback_key", config.FALLBACK_KEY, config.FALLBACK_INDEX_WHERE),
        _check_not_null(con, "PARID"),
        _check_not_null(con, "SALEDATE"),
        _check_not_null(con, "SALECODE"),
        _check_not_null(con, "INSTRTYP"),
        _check_not_null(con, "MUNICODE"),
        _check_price_not_negative(con),
        _check_recorded_before_sale_rate(con),
        _check_timestamp_consistency(con),
    ]
    failures = [c for c in result if not c["passed"]]
    if failures:
        raise CheckFailure(failures)
    return result
