"""Run the full pipeline: ingest -> stage -> check -> build mart -> check -> publish.

Nothing is written to output/ unless both check stages pass. A failed check is
printed and the script exits non-zero -- it does not overwrite a previously
published (and presumably good) output/ file.
"""
import sys

from pipeline import checks, config, db, ingest, transform


def main():
    con = db.connect()

    print("=== Ingest ===")
    result = ingest.ingest(con)
    print(f"status={result['status']} row_count={result['row_count']}")

    print("\n=== Transform: staging ===")
    try:
        n = transform.build_staging(con)
    except transform.SeedNotReadyError as e:
        print(f"FAILED: {e}")
        sys.exit(1)
    print(f"stg_sales: {n} rows")

    print("\n=== Checks (pre-mart) ===")
    try:
        for c in checks.run_pre_mart_checks(con):
            print(f"  [pass] {c['name']}: {c['detail']}")
    except checks.CheckFailure as e:
        for f in e.failures:
            print(f"  [FAIL] {f['name']}: {f['detail']}")
        print("\nAborting -- nothing published.")
        sys.exit(1)

    print("\n=== Transform: mart ===")
    n = transform.build_mart(con)
    print(f"mart_municipality_year: {n} rows")

    print("\n=== Checks (post-mart) ===")
    try:
        for c in checks.run_post_mart_checks(con):
            print(f"  [pass] {c['name']}: {c['detail']}")
    except checks.CheckFailure as e:
        for f in e.failures:
            print(f"  [FAIL] {f['name']}: {f['detail']}")
        print("\nAborting -- nothing published.")
        sys.exit(1)

    print("\n=== Publish ===")
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY mart_municipality_year TO '{config.OUTPUT_PATH.as_posix()}' (HEADER, DELIMITER ',')"
    )
    print(f"Wrote {config.OUTPUT_PATH}")


if __name__ == "__main__":
    main()
