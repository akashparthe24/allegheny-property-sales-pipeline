"""Entry point: `python run.py`. Four stages, stops at the first failure:
1. ingest (pre-load checks run inside it) -> 2. post-load checks ->
3. transform: clean -> 4. transform: mart (final output table).

Every run is logged to pipeline_runs/pipeline_errors -- see pipeline/db.py.
"""
import sys
import time

from pipeline import checks, db, ingest, transform


# Run all four stages in order; on any failure, log and stop.
def main():
    con = db.connect()
    run_id = db.start_run(con)
    started = time.monotonic()

    print(f"=== Run {run_id} ===")

    try:
        result = ingest.load_property_sales(con)
    except Exception as e:
        db.log_error(con, run_id, "ingest", type(e).__name__, str(e))
        db.finish_run_failed(con, run_id, "ingest")
        print(f"[FAILED] ingest: {type(e).__name__}: {e}")
        sys.exit(1)

    rows_loaded = result["rows_loaded"]
    row_count_check = result["row_count_check"]
    if row_count_check["overridden"]:
        print(f"  [OVERRIDE] {row_count_check['name']}: {row_count_check['detail']}")
        db.log_error(con, run_id, "ingest", f"{row_count_check['name']}_overridden", row_count_check["detail"])
    else:
        print(f"  [pass] {row_count_check['name']}: {row_count_check['detail']}")
    print(f"Loaded {rows_loaded} rows into {db.config.LOAD_TABLE}")

    print("\n=== Checks (post-load) ===")
    try:
        for c in checks.run_post_load_checks(con):
            print(f"  [pass] {c['name']}: {c['detail']}")
    except checks.CheckFailure as e:
        for f in e.failures:
            print(f"  [FAIL] {f['name']}: {f['detail']}")
            db.log_error(con, run_id, "post_load_checks", f["name"], f["detail"])
        db.finish_run_failed(con, run_id, "post_load_checks")
        print(f"\nAborting. (run_id={run_id})")
        sys.exit(1)

    print("\n=== Transform: clean ===")
    try:
        clean_n = transform.build_clean_table(con)
    except Exception as e:
        db.log_error(con, run_id, "transform_clean", type(e).__name__, str(e))
        db.finish_run_failed(con, run_id, "transform_clean")
        print(f"[FAILED] transform_clean: {type(e).__name__}: {e}")
        sys.exit(1)
    print(f"property_sales_clean: {clean_n} rows (from {rows_loaded} raw)")

    print("\n=== Transform: mart ===")
    try:
        transform.load_sale_codes_seed(con)
        mart_n = transform.build_mart_table(con)
    except Exception as e:
        db.log_error(con, run_id, "transform_mart", type(e).__name__, str(e))
        db.finish_run_failed(con, run_id, "transform_mart")
        print(f"[FAILED] transform_mart: {type(e).__name__}: {e}")
        sys.exit(1)
    print(f"property_sales_mart: {mart_n} rows")

    duration = round(time.monotonic() - started, 2)
    db.finish_run_success(con, run_id, rows_loaded, duration)
    print(f"\n=== Done (run_id={run_id}, {duration}s) ===")


if __name__ == "__main__":
    main()
