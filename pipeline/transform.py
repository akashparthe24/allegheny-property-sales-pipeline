"""Load the sale-code seed, then run the staging and mart SQL in order."""
from . import config


class SeedNotReadyError(RuntimeError):
    pass


def _load_seed(con, seed_path=None):
    path = seed_path or config.SEED_PATH
    if not path.exists():
        raise SeedNotReadyError(
            f"{path} does not exist yet. Run `make seed-draft` to generate a draft "
            "from the live data, review it against the sale codes, and save it as "
            "seeds/sale_codes.csv."
        )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE sale_codes_seed AS
        SELECT
            sale_code,
            sale_desc,
            CAST(is_market_sale AS BOOLEAN) AS is_market_sale,
            notes
        FROM read_csv('{path.as_posix()}', header = true, all_varchar = true)
        """
    )
    row_count = con.execute("SELECT COUNT(*) FROM sale_codes_seed").fetchone()[0]
    if row_count == 0:
        raise SeedNotReadyError(f"{path} exists but has no rows -- it needs to be filled in.")


def _run_sql_file(con, relative_path):
    sql_text = (config.SQL_DIR / relative_path).read_text()
    con.execute(sql_text)


def build_staging(con, seed_path=None):
    _load_seed(con, seed_path)
    _run_sql_file(con, "01_staging.sql")
    return con.execute("SELECT COUNT(*) FROM stg_sales").fetchone()[0]


def build_mart(con):
    _run_sql_file(con, "02_mart.sql")
    return con.execute("SELECT COUNT(*) FROM mart_municipality_year").fetchone()[0]
