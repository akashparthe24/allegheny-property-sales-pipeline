"""Download the sales CSV dump, save it untouched, and load it into DuckDB.

Design choices worth knowing about:
  - We always pull the *full* CSV dump (not the paginated datastore_search API).
    At ~506k rows / ~110MB it downloads in a few seconds, and the source system
    already does a full wipe-and-reload on its end (see package extras on
    data.wprdc.org), so there's no stable "give me only what changed" cursor to
    use anyway. Full refresh is simpler and correct.
  - We hash the download and skip re-loading if the content hasn't changed since
    the last successful run, so re-running the pipeline back-to-back is cheap and
    doesn't create duplicate work.
  - The raw file is saved byte-for-byte before any parsing, so a bad transform can
    always be re-run against exactly what the county published.
"""
import datetime
import hashlib

import requests

from . import config, db


class SchemaChangedError(RuntimeError):
    pass


def _download() -> bytes:
    resp = requests.get(config.CSV_DUMP_URL, timeout=120)
    resp.raise_for_status()
    return resp.content


def _check_header(content: bytes):
    header_line = content.split(b"\n", 1)[0].decode("utf-8")
    columns = [c.strip() for c in header_line.split(",")]
    missing = [c for c in config.REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise SchemaChangedError(
            f"Source file is missing expected columns {missing}. "
            "The county may have changed the file's shape -- stopping rather than "
            "guessing at a mapping."
        )


def _save_raw(content: bytes, checksum: str) -> "Path":
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.date.today().isoformat()
    path = config.RAW_DIR / f"sales_{stamp}_{checksum[:8]}.csv"
    path.write_bytes(content)
    return path


def _load_into_duckdb(con, raw_path):
    # strict_mode=false: DuckDB's CSV dialect sniffer cannot auto-detect this file
    # (confirmed against a live pull -- it errors out of the box). The rows parse
    # fine once strict mode is off; ignore_errors guards against any one bad line
    # breaking the whole load.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE raw_sales AS
        SELECT * FROM read_csv(
            '{raw_path.as_posix()}',
            all_varchar = true,
            strict_mode = false,
            ignore_errors = true
        )
        """
    )
    return con.execute("SELECT COUNT(*) FROM raw_sales").fetchone()[0]


def ingest(con) -> dict:
    run_id = db.next_run_id(con)
    content = _download()
    checksum = hashlib.sha256(content).hexdigest()

    _check_header(content)

    last_row = con.execute(
        "SELECT file_checksum FROM load_log WHERE status = 'downloaded' "
        "ORDER BY run_id DESC LIMIT 1"
    ).fetchone()
    previously_loaded_checksum = last_row[0] if last_row else None

    table_exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'raw_sales'"
    ).fetchone()[0] > 0

    if checksum == previously_loaded_checksum and table_exists:
        row_count = con.execute("SELECT COUNT(*) FROM raw_sales").fetchone()[0]
        db.log_run(con, run_id, config.RESOURCE_ID, checksum, row_count, "skipped_unchanged")
        return {"status": "skipped_unchanged", "row_count": row_count, "checksum": checksum}

    raw_path = _save_raw(content, checksum)
    row_count = _load_into_duckdb(con, raw_path)
    db.log_run(con, run_id, config.RESOURCE_ID, checksum, row_count, "downloaded")
    return {"status": "downloaded", "row_count": row_count, "checksum": checksum, "raw_path": str(raw_path)}
