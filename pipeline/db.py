"""DuckDB connection, plus three tracking tables:

  - load_log: one row per ingest *attempt* (download vs. skip-unchanged). Narrow and
    specific -- ingest.py reads it back to decide whether to skip a re-download.
  - pipeline_runs: one row per full `run.py` execution, covering every stage, not just
    ingest. Inserted at start (status='running'), updated in place at the end with
    either a 'success' summary (row counts at each stage, output path, headline stats)
    or a 'failed' status naming which stage broke.
  - pipeline_errors: one row per error *event*. Foreign-keyed to pipeline_runs.run_id
    so a single failed run that trips multiple quality checks at once still gets each
    failure recorded individually and is queryable later ("how often has check X failed").
"""
import duckdb

from . import config


def connect(db_path=None):
    path = db_path or config.DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    _ensure_log_table(con)
    _ensure_pipeline_run_tables(con)
    return con


def _ensure_log_table(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS load_log (
            run_id          BIGINT,
            started_at      TIMESTAMP,
            resource_id     VARCHAR,
            file_checksum   VARCHAR,
            row_count       BIGINT,
            status          VARCHAR,  -- 'downloaded', 'skipped_unchanged'
            note            VARCHAR
        )
        """
    )


def _ensure_pipeline_run_tables(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id                          BIGINT,
            started_at                      TIMESTAMP,
            finished_at                      TIMESTAMP,
            status                          VARCHAR,  -- 'running', 'success', 'failed'
            failed_stage                     VARCHAR,  -- ingest/staging/pre_checks/mart/post_checks/publish
            source_checksum                  VARCHAR,
            raw_row_count                    BIGINT,
            staged_row_count                 BIGINT,
            mart_row_count                   BIGINT,
            output_path                      VARCHAR,
            pct_market_sale_overall          DOUBLE,
            median_market_price_overall      DOUBLE,
            duration_seconds                 DOUBLE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_errors (
            error_id        BIGINT,
            run_id          BIGINT,
            occurred_at     TIMESTAMP,
            stage           VARCHAR,
            error_type      VARCHAR,
            message         VARCHAR
        )
        """
    )


def next_run_id(con) -> int:
    row = con.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM load_log").fetchone()
    return row[0]


def log_run(con, run_id, resource_id, checksum, row_count, status, note=""):
    con.execute(
        """
        INSERT INTO load_log (run_id, started_at, resource_id, file_checksum, row_count, status, note)
        VALUES (?, now(), ?, ?, ?, ?, ?)
        """,
        [run_id, resource_id, checksum, row_count, status, note],
    )


def last_successful_row_count(con) -> int | None:
    row = con.execute(
        """
        SELECT row_count FROM load_log
        WHERE status = 'downloaded'
        ORDER BY run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return row[0] if row else None


def start_pipeline_run(con, run_id):
    con.execute(
        """
        INSERT INTO pipeline_runs (run_id, started_at, status)
        VALUES (?, now(), 'running')
        """,
        [run_id],
    )


def finish_pipeline_run_success(con, run_id, *, source_checksum, raw_row_count,
                                 staged_row_count, mart_row_count, output_path,
                                 pct_market_sale_overall, median_market_price_overall,
                                 duration_seconds):
    con.execute(
        """
        UPDATE pipeline_runs SET
            finished_at = now(),
            status = 'success',
            source_checksum = ?,
            raw_row_count = ?,
            staged_row_count = ?,
            mart_row_count = ?,
            output_path = ?,
            pct_market_sale_overall = ?,
            median_market_price_overall = ?,
            duration_seconds = ?
        WHERE run_id = ?
        """,
        [source_checksum, raw_row_count, staged_row_count, mart_row_count, output_path,
         pct_market_sale_overall, median_market_price_overall, duration_seconds, run_id],
    )


def finish_pipeline_run_failed(con, run_id, failed_stage):
    con.execute(
        """
        UPDATE pipeline_runs SET
            finished_at = now(),
            status = 'failed',
            failed_stage = ?
        WHERE run_id = ?
        """,
        [failed_stage, run_id],
    )


def next_error_id(con) -> int:
    row = con.execute("SELECT COALESCE(MAX(error_id), 0) + 1 FROM pipeline_errors").fetchone()
    return row[0]


def log_error(con, run_id, stage, error_type, message):
    error_id = next_error_id(con)
    con.execute(
        """
        INSERT INTO pipeline_errors (error_id, run_id, occurred_at, stage, error_type, message)
        VALUES (?, ?, now(), ?, ?, ?)
        """,
        [error_id, run_id, stage, error_type, message],
    )
