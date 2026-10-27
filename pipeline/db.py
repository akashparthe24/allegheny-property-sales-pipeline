"""DuckDB connection + the load_log table that records every ingest attempt."""
import duckdb

from . import config


def connect(db_path=None):
    path = db_path or config.DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    _ensure_log_table(con)
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
            status          VARCHAR,  -- 'downloaded', 'skipped_unchanged', 'failed'
            note            VARCHAR
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
