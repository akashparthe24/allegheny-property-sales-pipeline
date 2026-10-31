"""SQLite connection, plus two tracking tables: pipeline_runs (one row per
pipeline execution) and pipeline_errors (one row per error/failure event,
linked to pipeline_runs by run_id)."""
import sqlite3

from . import config


# Open/create the warehouse db, ensure the audit tables exist, clean up any
# zombie 'running' row left by a crashed process.
def connect(db_path=None):
    path = db_path or config.DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None -> autocommit; every statement takes effect immediately.
    con = sqlite3.connect(str(path), isolation_level=None)
    _ensure_tables(con)
    _mark_orphaned_runs(con)
    return con


# CREATE TABLE IF NOT EXISTS for both audit tables.
def _ensure_tables(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at      TEXT,
            finished_at     TEXT,
            status          TEXT,     -- 'running', 'success', 'failed'
            failed_stage    TEXT,     -- which stage broke, e.g. 'ingest' (NULL on success)
            rows_loaded     INTEGER,
            duration_seconds REAL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_errors (
            error_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER,
            occurred_at     TEXT,
            stage           TEXT,
            error_type      TEXT,
            message         TEXT
        )
        """
    )


# A row still 'running' at connect() time means a previous process died
# mid-run -- mark it failed/orphaned instead of leaving it stuck forever.
def _mark_orphaned_runs(con):
    con.execute(
        """
        UPDATE pipeline_runs
        SET status = 'failed', failed_stage = 'orphaned', finished_at = datetime('now')
        WHERE status = 'running'
        """
    )


# Insert a new 'running' row, return its run_id.
def start_run(con) -> int:
    cur = con.execute(
        "INSERT INTO pipeline_runs (started_at, status) VALUES (datetime('now'), 'running')"
    )
    return cur.lastrowid


# Mark a run successful, with its summary numbers.
def finish_run_success(con, run_id, rows_loaded, duration_seconds):
    con.execute(
        """
        UPDATE pipeline_runs
        SET finished_at = datetime('now'), status = 'success',
            rows_loaded = ?, duration_seconds = ?
        WHERE run_id = ?
        """,
        [rows_loaded, duration_seconds, run_id],
    )


# Mark a run failed, naming which stage broke.
def finish_run_failed(con, run_id, failed_stage):
    con.execute(
        """
        UPDATE pipeline_runs
        SET finished_at = datetime('now'), status = 'failed', failed_stage = ?
        WHERE run_id = ?
        """,
        [failed_stage, run_id],
    )


# Record one error/failure event, linked to run_id.
def log_error(con, run_id, stage, error_type, message):
    con.execute(
        """
        INSERT INTO pipeline_errors (run_id, occurred_at, stage, error_type, message)
        VALUES (?, datetime('now'), ?, ?, ?)
        """,
        [run_id, stage, error_type, message],
    )
