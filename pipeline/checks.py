"""Data quality checks. Each one returns a dict with name/passed/detail; nothing
gets published (see run.py) if any check in either stage fails."""
from . import config


class CheckFailure(RuntimeError):
    def __init__(self, failures):
        self.failures = failures
        detail = "; ".join(f"{f['name']}: {f['detail']}" for f in failures)
        super().__init__(f"{len(failures)} check(s) failed: {detail}")


def _check_row_count_not_zero(con):
    n = con.execute("SELECT COUNT(*) FROM stg_sales").fetchone()[0]
    return {"name": "row_count_not_zero", "passed": n > 0, "detail": f"{n} rows in stg_sales"}


def _check_no_duplicate_natural_key(con):
    n = con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT parcel_id, record_date, sale_date, deed_book, deed_page,
                   instrument_type, price, sale_code, COUNT(*) c
            FROM stg_sales
            GROUP BY 1,2,3,4,5,6,7,8
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    return {
        "name": "no_duplicate_natural_key",
        "passed": n == 0,
        "detail": f"{n} duplicate natural-key groups found after de-dup" if n else "ok",
    }


def _check_unknown_sale_codes(con):
    rows = con.execute(
        "SELECT DISTINCT sale_code FROM stg_sales WHERE NOT is_known_code"
    ).fetchall()
    codes = [r[0] for r in rows]
    return {
        "name": "unknown_sale_codes",
        "passed": len(codes) == 0,
        "detail": (
            f"{len(codes)} sale code(s) present in the data but missing from "
            f"seeds/sale_codes.csv: {codes}. Add them (run `make seed-draft` to see "
            "counts) before this can pass."
            if codes else "ok"
        ),
    }


def _check_lowprice_in_market_sales(con):
    row = con.execute(
        """
        SELECT
            SUM(CASE WHEN is_market_sale THEN 1 ELSE 0 END) AS market_n,
            SUM(CASE WHEN is_market_sale AND is_zero_or_one_dollar THEN 1 ELSE 0 END) AS bad_n
        FROM stg_sales
        """
    ).fetchone()
    market_n, bad_n = row[0] or 0, row[1] or 0
    pct = (100.0 * bad_n / market_n) if market_n else 0.0
    threshold = config.MAX_LOWPRICE_IN_MARKET_SALES_PCT
    return {
        "name": "lowprice_in_market_sales_rate",
        "passed": pct <= threshold,
        "detail": (
            f"{bad_n}/{market_n} ({pct:.3f}%) of rows coded as market sales have a "
            f"$0/$1 price, vs. the {threshold}% allowed. A small nonzero rate is "
            "expected (the county's own validation misses a handful of clerical "
            "errors); a rate this high usually means the sale-code join broke."
        ),
    }


def _check_row_count_vs_previous(con):
    current = con.execute("SELECT COUNT(*) FROM raw_sales").fetchone()[0]
    row = con.execute(
        "SELECT row_count FROM load_log WHERE status = 'downloaded' "
        "ORDER BY run_id DESC LIMIT 2"
    ).fetchall()
    # row[0] is this run's own just-logged entry (if ingest ran); row[1] is the prior one.
    previous = row[1][0] if len(row) > 1 else None
    if previous is None or previous == 0:
        return {"name": "row_count_vs_previous_run", "passed": True, "detail": "no prior run to compare against"}
    drop_pct = 100.0 * (previous - current) / previous
    threshold = config.MAX_ROW_COUNT_DROP_PCT
    exceeds = drop_pct > threshold
    overridden = exceeds and config.ALLOW_ROW_COUNT_DROP_OVERRIDE
    if not exceeds:
        detail = f"{current} rows vs. {previous} previously (+{-drop_pct:.1f}%)"
    elif overridden:
        detail = (
            f"row count dropped {drop_pct:.1f}% vs. previous run ({previous} -> {current}), "
            f"exceeding the {threshold}% allowance -- ALLOW_ROW_COUNT_DROP override applied, "
            "treat this as a new baseline only after independently confirming the drop "
            "against the source (e.g. datastore_search 'total' and the resource's "
            "last_modified timestamp)"
        )
    else:
        detail = (
            f"row count dropped {drop_pct:.1f}% vs. previous run ({previous} -> {current}), "
            f"exceeding the {threshold}% allowance -- looks like a partial download"
        )
    return {
        "name": "row_count_vs_previous_run",
        "passed": not exceeds or overridden,
        "overridden": overridden,
        "detail": detail,
    }


def run_pre_mart_checks(con):
    checks = [
        _check_row_count_not_zero(con),
        _check_no_duplicate_natural_key(con),
        _check_unknown_sale_codes(con),
        _check_lowprice_in_market_sales(con),
        _check_row_count_vs_previous(con),
    ]
    failures = [c for c in checks if not c["passed"]]
    if failures:
        raise CheckFailure(failures)
    return checks


def _check_mart_not_empty(con):
    n = con.execute("SELECT COUNT(*) FROM mart_municipality_year").fetchone()[0]
    return {"name": "mart_not_empty", "passed": n > 0, "detail": f"{n} rows"}


def _check_pct_market_sale_in_range(con):
    n = con.execute(
        "SELECT COUNT(*) FROM mart_municipality_year "
        "WHERE pct_market_sale < 0 OR pct_market_sale > 100"
    ).fetchone()[0]
    return {
        "name": "pct_market_sale_in_range",
        "passed": n == 0,
        "detail": f"{n} rows with pct_market_sale outside [0, 100]" if n else "ok",
    }


def run_post_mart_checks(con):
    checks = [
        _check_mart_not_empty(con),
        _check_pct_market_sale_in_range(con),
    ]
    failures = [c for c in checks if not c["passed"]]
    if failures:
        raise CheckFailure(failures)
    return checks
