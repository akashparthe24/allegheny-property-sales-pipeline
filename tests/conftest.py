import pytest

from pipeline import config, db

# The subset of real source columns the pipeline actually touches.
RAW_COLUMNS = [
    "PARID", "MUNICODE", "MUNIDESC", "RECORDDATE", "SALEDATE",
    "PRICE", "DEEDBOOK", "DEEDPAGE", "SALECODE", "SALEDESC", "INSTRTYP",
]


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "RAW_DIR", tmp_path / "data" / "raw")
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "data" / "warehouse.duckdb")
    monkeypatch.setattr(config, "OUTPUT_DIR", tmp_path / "output")
    monkeypatch.setattr(config, "OUTPUT_PATH", tmp_path / "output" / "municipality_year_summary.csv")
    con = db.connect()
    yield con
    con.close()


def load_raw_rows(con, rows):
    """rows: list of tuples in RAW_COLUMNS order. Replaces raw_sales entirely."""
    cols_def = ", ".join(f"{c} VARCHAR" for c in RAW_COLUMNS)
    con.execute(f"CREATE OR REPLACE TABLE raw_sales ({cols_def})")
    placeholders = ", ".join(["?"] * len(RAW_COLUMNS))
    con.executemany(f"INSERT INTO raw_sales VALUES ({placeholders})", rows)


def write_seed(path, rows):
    """rows: list of (sale_code, sale_desc, is_market_sale, notes)."""
    with open(path, "w") as f:
        f.write("sale_code,sale_desc,is_market_sale,notes\n")
        for code, desc, is_market, notes in rows:
            f.write(f"{code},{desc},{str(is_market).lower()},{notes}\n")


DEFAULT_SEED_ROWS = [
    ("0", "VALID SALE", True, "baseline"),
    ("3", "LOVE AND AFFECTION SALE", False, "family transfer"),
]

STG_COLUMNS = [
    "parcel_id", "record_date", "sale_date", "deed_book", "deed_page",
    "instrument_type", "price", "sale_code", "is_market_sale", "is_known_code",
    "is_zero_or_one_dollar", "recorded_before_sale",
]


def make_stg_sales(con, rows):
    """rows: list of tuples in STG_COLUMNS order (a direct stand-in for stg_sales,
    bypassing the staging SQL so checks.py can be tested in isolation)."""
    con.execute(
        """
        CREATE OR REPLACE TABLE stg_sales (
            parcel_id VARCHAR, record_date DATE, sale_date DATE, deed_book VARCHAR,
            deed_page VARCHAR, instrument_type VARCHAR, price DOUBLE, sale_code VARCHAR,
            is_market_sale BOOLEAN, is_known_code BOOLEAN, is_zero_or_one_dollar BOOLEAN,
            recorded_before_sale BOOLEAN
        )
        """
    )
    placeholders = ", ".join(["?"] * len(STG_COLUMNS))
    con.executemany(f"INSERT INTO stg_sales VALUES ({placeholders})", rows)
