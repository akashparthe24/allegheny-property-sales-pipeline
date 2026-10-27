from pipeline import config, transform
from conftest import load_raw_rows, write_seed, DEFAULT_SEED_ROWS


def test_build_staging_dedupes_exact_natural_key_repeats(warehouse, tmp_path):
    load_raw_rows(warehouse, [
        ("P1", "100", "Test Muni", "2020-01-05", "2020-01-01", "100000", "100", "1", "0", "VALID SALE", "DE"),
        ("P1", "100", "Test Muni", "2020-01-05", "2020-01-01", "100000", "100", "1", "0", "VALID SALE", "DE"),  # exact repeat
    ])
    seed_path = tmp_path / "sale_codes.csv"
    write_seed(seed_path, DEFAULT_SEED_ROWS)

    n = transform.build_staging(warehouse, seed_path=seed_path)
    assert n == 1


def test_build_staging_flags_unknown_codes(warehouse, tmp_path):
    load_raw_rows(warehouse, [
        ("P1", "100", "Test Muni", "2020-01-05", "2020-01-01", "100000", "100", "1", "ZZ", "MYSTERY CODE", "DE"),
    ])
    seed_path = tmp_path / "sale_codes.csv"
    write_seed(seed_path, DEFAULT_SEED_ROWS)  # does not contain "ZZ"

    transform.build_staging(warehouse, seed_path=seed_path)
    is_known = warehouse.execute("SELECT is_known_code FROM stg_sales WHERE parcel_id = 'P1'").fetchone()[0]
    assert is_known is False


def test_build_staging_drops_rows_with_unparseable_sale_date(warehouse, tmp_path):
    load_raw_rows(warehouse, [
        ("P1", "100", "Test Muni", "2020-01-05", "not-a-date", "100000", "100", "1", "0", "VALID SALE", "DE"),
        ("P2", "100", "Test Muni", "2020-01-05", "2020-01-01", "100000", "101", "1", "0", "VALID SALE", "DE"),
    ])
    seed_path = tmp_path / "sale_codes.csv"
    write_seed(seed_path, DEFAULT_SEED_ROWS)

    n = transform.build_staging(warehouse, seed_path=seed_path)
    assert n == 1


def test_mart_computes_median_price_and_year_over_year_change(warehouse, tmp_path):
    load_raw_rows(warehouse, [
        ("P1", "100", "Test Muni", "2020-01-05", "2020-01-01", "100000", "100", "1", "0", "VALID SALE", "DE"),
        ("P2", "100", "Test Muni", "2021-02-05", "2021-02-01", "150000", "101", "1", "0", "VALID SALE", "DE"),
        ("P3", "100", "Test Muni", "2021-03-05", "2021-03-01", "1", "102", "1", "3", "LOVE AND AFFECTION SALE", "DE"),
    ])
    seed_path = tmp_path / "sale_codes.csv"
    write_seed(seed_path, DEFAULT_SEED_ROWS)
    transform.build_staging(warehouse, seed_path=seed_path)
    transform.build_mart(warehouse)

    rows = {r[0]: r for r in warehouse.execute(
        "SELECT sale_year, total_sale_count, market_sale_count, median_market_price, "
        "yoy_median_price_change_pct FROM mart_municipality_year ORDER BY sale_year"
    ).fetchall()}

    assert rows[2020][1:4] == (1, 1, 100000.0)
    assert rows[2020][4] is None  # no prior year to compare against
    assert rows[2021][1:4] == (2, 1, 150000.0)  # the $1 love-and-affection sale isn't a market sale
    assert rows[2021][4] == 50.0  # (150000 - 100000) / 100000 * 100
