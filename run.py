"""Run the full pipeline: ingest -> stage -> check -> build mart -> check -> publish.

Nothing is written to output/ unless both check stages pass. A failed check is
printed and the script exits non-zero -- it does not overwrite a previously
published (and presumably good) output/ file.

Every execution is tracked in two DuckDB tables (see pipeline/db.py):
  - pipeline_runs: one row for this run, started as 'running' and updated at the
    end to 'success' (with row counts/output path/headline stats) or 'failed'
    (with the stage that broke).
  - pipeline_errors: one row per error/failed-check event, so a run that trips
    several quality checks at once still records each one individually.
"""
import sys
import time

from pipeline import checks, config, db, ingest, transform


def main():
    con = db.connect()
    run_id = db.next_run_id(con)
    started = time.monotonic()
    db.start_pipeline_run(con, run_id)

    def abort(stage, error_type, message):
        db.log_error(con, run_id, stage, error_type, message)
        db.finish_pipeline_run_failed(con, run_id, stage)
        print(f"\nAborting at stage '{stage}' -- nothing published. (run_id={run_id})")
        sys.exit(1)

    print("=== Ingest ===")
    try:
        result = ingest.ingest(con, run_id=run_id)
    except Exception as e:
        abort("ingest", type(e).__name__, str(e))
    print(f"status={result['status']} row_count={result['row_count']}")

    print("\n=== Transform: staging ===")
    try:
        staged_n = transform.build_staging(con)
    except transform.SeedNotReadyError as e:
        abort("staging", type(e).__name__, str(e))
    print(f"stg_sales: {staged_n} rows")

    print("\n=== Checks (pre-mart) ===")
    try:
        for c in checks.run_pre_mart_checks(con):
            if c.get("overridden"):
                print(f"  [OVERRIDE] {c['name']}: {c['detail']}")
                db.log_error(con, run_id, "pre_checks", f"{c['name']}_overridden", c["detail"])
            else:
                print(f"  [pass] {c['name']}: {c['detail']}")
    except checks.CheckFailure as e:
        for f in e.failures:
            print(f"  [FAIL] {f['name']}: {f['detail']}")
            db.log_error(con, run_id, "pre_checks", f["name"], f["detail"])
        db.finish_pipeline_run_failed(con, run_id, "pre_checks")
        print(f"\nAborting -- nothing published. (run_id={run_id})")
        sys.exit(1)

    print("\n=== Transform: mart ===")
    try:
        mart_n = transform.build_mart(con)
    except Exception as e:
        abort("mart", type(e).__name__, str(e))
    print(f"mart_municipality_year: {mart_n} rows")

    print("\n=== Checks (post-mart) ===")
    try:
        for c in checks.run_post_mart_checks(con):
            print(f"  [pass] {c['name']}: {c['detail']}")
    except checks.CheckFailure as e:
        for f in e.failures:
            print(f"  [FAIL] {f['name']}: {f['detail']}")
            db.log_error(con, run_id, "post_checks", f["name"], f["detail"])
        db.finish_pipeline_run_failed(con, run_id, "post_checks")
        print(f"\nAborting -- nothing published. (run_id={run_id})")
        sys.exit(1)

    print("\n=== Publish ===")
    try:
        config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY mart_municipality_year TO '{config.OUTPUT_PATH.as_posix()}' (HEADER, DELIMITER ',')"
        )
    except Exception as e:
        abort("publish", type(e).__name__, str(e))
    print(f"Wrote {config.OUTPUT_PATH}")

    overall_pct_market, overall_median_price = con.execute(
        """
        SELECT
            ROUND(100.0 * SUM(market_sale_count) / SUM(total_sale_count), 1),
            MEDIAN(median_market_price)
        FROM mart_municipality_year
        """
    ).fetchone()
    duration = round(time.monotonic() - started, 2)
    db.finish_pipeline_run_success(
        con, run_id,
        source_checksum=result["checksum"],
        raw_row_count=result["row_count"],
        staged_row_count=staged_n,
        mart_row_count=mart_n,
        output_path=str(config.OUTPUT_PATH),
        pct_market_sale_overall=overall_pct_market,
        median_market_price_overall=overall_median_price,
        duration_seconds=duration,
    )
    print(f"\n=== Done (run_id={run_id}, {duration}s) ===")


if __name__ == "__main__":
    main()
