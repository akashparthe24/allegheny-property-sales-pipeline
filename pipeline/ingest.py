"""Load property sale records straight from WPRDC's CKAN API into SQLite --
no CSV file is ever downloaded or saved.

Upsert key: (PARID, DEEDBOOK, DEEDPAGE) -- not PRICE/SALECODE, since those are
exactly the fields the county corrects after the fact; including them in the
key would make a correction look like a new row instead of an update.

Fallback key: ~0.1% of rows have a blank DEEDBOOK/DEEDPAGE. SQLite treats two
NULLs as never equal, so those rows need the wider 8-column key WPRDC's own ETL
uses instead. Each key has its own partial unique index and its own upsert
statement -- see _ensure_table_and_indexes and _build_upsert_sql.

_id (CKAN's internal row id) is excluded from the key and from change-detection:
it regenerates on a full source reload, so comparing it would make every row
look "changed" every run.

Two checks (pipeline/checks.py) run against just the first API page, before the
real load starts -- a schema change or a suspicious row-count swing aborts here,
before property_sales_load is touched.
"""
import requests

from . import checks, config

AUDIT_COLUMNS = ["INSERT_TIMESTAMP", "UPDATE_TIMESTAMP"]
MAIN_KEY = config.MAIN_KEY
FALLBACK_KEY = config.FALLBACK_KEY
IGNORE_FOR_COMPARISON = {"_id"}  # churns on every source reload; not a real value change

# Must match the partial index definitions exactly -- SQLite requires an upsert's
# ON CONFLICT target to repeat a partial index's WHERE clause verbatim.
MAIN_INDEX_WHERE = config.MAIN_INDEX_WHERE
FALLBACK_INDEX_WHERE = config.FALLBACK_INDEX_WHERE


# One page of records from the live API, starting at `offset`.
def _fetch_page(offset: int) -> dict:
    resp = requests.get(
        config.DATASTORE_SEARCH_URL,
        params={
            "resource_id": config.RESOURCE_ID,
            "limit": config.PAGE_SIZE,
            "offset": offset,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["result"]


# Quote a column name, e.g. PARID -> "PARID".
def _q(name: str) -> str:
    return f'"{name}"'


# Quote and comma-join a list of names.
def _qjoin(names) -> str:
    return ", ".join(_q(n) for n in names)


# Create property_sales_load and its two partial unique indexes if they don't
# exist yet. Partial, so the main index ignores blank-deed rows and vice versa --
# otherwise they'd collide with each other across unrelated PARIDs.
def _ensure_table_and_indexes(con, columns):
    columns_sql = ", ".join(f"{_q(c)} TEXT" for c in columns)
    audit_columns_sql = ", ".join(f"{_q(c)} TEXT" for c in AUDIT_COLUMNS)
    con.execute(f'CREATE TABLE IF NOT EXISTS {config.LOAD_TABLE} ({columns_sql}, {audit_columns_sql})')
    con.execute(
        f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{config.LOAD_TABLE}_main '
        f'ON {config.LOAD_TABLE} ({_qjoin(MAIN_KEY)}) '
        f'WHERE {MAIN_INDEX_WHERE}'
    )
    con.execute(
        f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{config.LOAD_TABLE}_fallback '
        f'ON {config.LOAD_TABLE} ({_qjoin(FALLBACK_KEY)}) '
        f'WHERE {FALLBACK_INDEX_WHERE}'
    )


# Build one INSERT ... ON CONFLICT ... DO UPDATE statement for a given key.
# Updates only fire if a non-key value actually differs -- an unchanged row is
# left completely untouched, including its UPDATE_TIMESTAMP.
def _build_upsert_sql(all_columns, key_columns, index_where):
    compare_columns = [c for c in all_columns if c not in key_columns and c not in IGNORE_FOR_COMPARISON]
    insert_columns = all_columns + AUDIT_COLUMNS
    placeholders = ", ".join(["?"] * len(insert_columns))
    set_clause = ", ".join(f"{_q(c)} = excluded.{_q(c)}" for c in compare_columns)
    set_clause += f', {_q("UPDATE_TIMESTAMP")} = excluded.{_q("UPDATE_TIMESTAMP")}'
    changed_clause = " OR ".join(f"{_q(c)} IS NOT excluded.{_q(c)}" for c in compare_columns)
    return (
        f"INSERT INTO {config.LOAD_TABLE} ({_qjoin(insert_columns)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT({_qjoin(key_columns)}) WHERE {index_where} DO UPDATE SET {set_clause} "
        f"WHERE {changed_clause}"
    )


# Entry point for the ingest stage: fetch page 1 -> pre-load checks -> ensure
# table/indexes -> page through the rest, upserting each page, all in one
# transaction. Returns {"rows_loaded": int, "row_count_check": dict}.
def load_property_sales(con) -> dict:
    first_page = _fetch_page(offset=0)
    columns = [f["id"] for f in first_page["fields"]]
    total = first_page["total"]

    checks.check_required_columns(columns)
    row_count_check = checks.check_row_count_vs_previous(con, total)

    _ensure_table_and_indexes(con, columns)
    main_sql = _build_upsert_sql(columns, MAIN_KEY, MAIN_INDEX_WHERE)
    fallback_sql = _build_upsert_sql(columns, FALLBACK_KEY, FALLBACK_INDEX_WHERE)

    # One timestamp per run, not per row. On a new row it fills both
    # INSERT_TIMESTAMP/UPDATE_TIMESTAMP; on an updated row only UPDATE_TIMESTAMP
    # is written, so INSERT_TIMESTAMP keeps recording when the row first appeared.
    loaded_at = con.execute("SELECT datetime('now')").fetchone()[0]

    def has_deed_info(r):
        return bool(r.get("DEEDBOOK")) and bool(r.get("DEEDPAGE"))

    def row_values(r, key_columns):
        # Normalize NULL -> '' only in key columns, so two blank-deed rows can
        # still match each other under the unique index (NULL never equals NULL).
        return tuple(
            ("" if c in key_columns else None) if r.get(c) is None else str(r.get(c))
            for c in columns
        ) + (loaded_at, loaded_at)

    con.execute("BEGIN")
    loaded = 0
    offset = 0
    while True:
        page = first_page if offset == 0 else _fetch_page(offset=offset)
        if not page["records"]:
            break

        main_rows = [row_values(r, MAIN_KEY) for r in page["records"] if has_deed_info(r)]
        fallback_rows = [row_values(r, FALLBACK_KEY) for r in page["records"] if not has_deed_info(r)]
        if main_rows:
            con.executemany(main_sql, main_rows)
        if fallback_rows:
            con.executemany(fallback_sql, fallback_rows)

        loaded += len(page["records"])
        offset += config.PAGE_SIZE
        if offset >= total:
            break
    con.execute("COMMIT")

    return {"rows_loaded": loaded, "row_count_check": row_count_check}
