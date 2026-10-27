"""Exploration tool: pull the live data and print what it actually looks like
(sale codes, $0/$1 transfers, date weirdness, duplicates), and optionally draft
seeds/sale_codes.csv from the real distribution of codes.

This is deliberately independent of run.py / the DuckDB warehouse -- it's meant to
be the first thing you run, before a seed file exists at all.
"""
import argparse
import csv

import duckdb

from . import config
from .ingest import _download, _check_header  # reuse the same download/validation logic

PROFILE_CSV = config.RAW_DIR / "profile_pull.csv"


def _load_fresh_pull() -> duckdb.DuckDBPyConnection:
    content = _download()
    _check_header(content)
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROFILE_CSV.write_bytes(content)

    con = duckdb.connect()  # in-memory, not the real warehouse
    con.execute(
        f"""
        CREATE TABLE raw AS
        SELECT * FROM read_csv(
            '{PROFILE_CSV.as_posix()}',
            all_varchar = true,
            strict_mode = false,
            ignore_errors = true
        )
        """
    )
    return con


def _print_overview(con):
    total = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    date_range = con.execute(
        "SELECT MIN(SALEDATE), MAX(SALEDATE) FROM raw WHERE SALEDATE != ''"
    ).fetchone()
    n_munis = con.execute("SELECT COUNT(DISTINCT MUNICODE) FROM raw").fetchone()[0]
    null_price = con.execute("SELECT COUNT(*) FROM raw WHERE PRICE IS NULL OR PRICE=''").fetchone()[0]
    zero_price = con.execute("SELECT COUNT(*) FROM raw WHERE TRY_CAST(PRICE AS DOUBLE)=0").fetchone()[0]
    one_price = con.execute("SELECT COUNT(*) FROM raw WHERE TRY_CAST(PRICE AS DOUBLE)=1").fetchone()[0]
    recorded_before_sale = con.execute(
        "SELECT COUNT(*) FROM raw WHERE TRY_CAST(RECORDDATE AS DATE) < TRY_CAST(SALEDATE AS DATE)"
    ).fetchone()[0]
    bad_record_date = con.execute(
        "SELECT COUNT(*) FROM raw WHERE RECORDDATE != '' AND TRY_CAST(RECORDDATE AS DATE) IS NULL"
    ).fetchone()[0]
    dup_groups = con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT PARID, RECORDDATE, SALEDATE, DEEDBOOK, DEEDPAGE, INSTRTYP, PRICE, SALECODE, COUNT(*) c
            FROM raw GROUP BY 1,2,3,4,5,6,7,8 HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    print("=== Overview ===")
    print(f"total rows:                 {total:,}")
    print(f"sale_date range:            {date_range[0]} .. {date_range[1]}")
    print(f"distinct municipality codes:{n_munis}  (note: Pittsburgh/McKeesport/Duquesne/"
          f"Clairton are split into wards, each with its own code)")
    print(f"null price:                 {null_price:,}")
    print(f"$0 price:                   {zero_price:,} ({100*zero_price/total:.1f}%)")
    print(f"$1 price:                   {one_price:,} ({100*one_price/total:.1f}%)")
    print(f"recorded before sale date:  {recorded_before_sale:,} (logically impossible -- data errors)")
    print(f"unparseable record_date:    {bad_record_date:,}")
    print(f"duplicate natural-key rows: {dup_groups:,}")


def _sale_code_breakdown(con):
    rows = con.execute(
        "SELECT SALECODE, SALEDESC, COUNT(*) c FROM raw GROUP BY 1,2 ORDER BY c DESC"
    ).fetchall()
    print("\n=== Sale codes (code, description, count) ===")
    for code, desc, count in rows:
        print(f"{code:>6}  {desc:<45} {count:>8,}")
    return rows


def _write_draft(rows):
    config.DRAFT_SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(config.DRAFT_SEED_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sale_code", "sale_desc", "sale_count", "is_market_sale", "notes"])
        for code, desc, count in rows:
            writer.writerow([code, desc, count, "", ""])
    print(f"\nWrote draft to {config.DRAFT_SEED_PATH}")
    print("Review it, fill in is_market_sale (true/false) and notes for each code, "
          "then save as seeds/sale_codes.csv (without the sale_count column).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-draft", action="store_true")
    args = parser.parse_args()

    con = _load_fresh_pull()
    _print_overview(con)
    rows = _sale_code_breakdown(con)
    if args.write_draft:
        _write_draft(rows)


if __name__ == "__main__":
    main()
