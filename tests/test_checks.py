import pytest

from pipeline import checks
from conftest import make_stg_sales, load_raw_rows

GOOD_ROW = ("P1", "2020-01-05", "2020-01-01", "100", "1", "DE", 100000.0, "0", True, True, False, False)


def test_unknown_sale_code_check_fails(warehouse):
    make_stg_sales(warehouse, [GOOD_ROW, ("P2", "2020-01-05", "2020-01-01", "101", "1", "DE", 5000.0, "ZZ", False, False, False, False)])
    result = checks._check_unknown_sale_codes(warehouse)
    assert result["passed"] is False
    assert "ZZ" in result["detail"]


def test_unknown_sale_code_check_passes_when_all_codes_known(warehouse):
    make_stg_sales(warehouse, [GOOD_ROW])
    result = checks._check_unknown_sale_codes(warehouse)
    assert result["passed"] is True


def test_duplicate_natural_key_check_fails(warehouse):
    make_stg_sales(warehouse, [GOOD_ROW, GOOD_ROW])  # identical row twice
    result = checks._check_no_duplicate_natural_key(warehouse)
    assert result["passed"] is False


def test_duplicate_natural_key_check_passes(warehouse):
    make_stg_sales(warehouse, [GOOD_ROW])
    result = checks._check_no_duplicate_natural_key(warehouse)
    assert result["passed"] is True


def test_lowprice_in_market_sales_fails_when_rate_is_high(warehouse):
    bad_rows = [
        ("P%d" % i, "2020-01-05", "2020-01-01", str(i), "1", "DE", 1.0, "0", True, True, True, False)
        for i in range(5)
    ]
    good_rows = [
        ("G%d" % i, "2020-01-05", "2020-01-01", str(i), "1", "DE", 100000.0, "0", True, True, False, False)
        for i in range(5)
    ]
    make_stg_sales(warehouse, bad_rows + good_rows)  # 50% of market sales are $1 -- way above threshold
    result = checks._check_lowprice_in_market_sales(warehouse)
    assert result["passed"] is False


def test_lowprice_in_market_sales_passes_when_rate_is_low(warehouse):
    rows = [
        ("G%d" % i, "2020-01-05", "2020-01-01", str(i), "1", "DE", 100000.0, "0", True, True, False, False)
        for i in range(200)
    ]
    rows[0] = ("P0", "2020-01-05", "2020-01-01", "0", "1", "DE", 1.0, "0", True, True, True, False)  # 1 in 200 = 0.5%
    make_stg_sales(warehouse, rows)
    result = checks._check_lowprice_in_market_sales(warehouse)
    assert result["passed"] is True


def test_row_count_vs_previous_run_fails_on_big_drop(warehouse):
    load_raw_rows(warehouse, [
        ("P1", "100", "Test Muni", "2020-01-05", "2020-01-01", "100000", "100", "1", "0", "VALID SALE", "DE"),
    ])
    run_id = 1
    warehouse.execute(
        "INSERT INTO load_log (run_id, started_at, resource_id, file_checksum, row_count, status, note) "
        "VALUES (?, now(), 'r', 'old', ?, 'downloaded', '')",
        [run_id, 1000],
    )
    warehouse.execute(
        "INSERT INTO load_log (run_id, started_at, resource_id, file_checksum, row_count, status, note) "
        "VALUES (?, now(), 'r', 'new', ?, 'downloaded', '')",
        [run_id + 1, 1],  # this run's raw_sales only has 1 row -- a 99.9% drop
    )
    result = checks._check_row_count_vs_previous(warehouse)
    assert result["passed"] is False


def test_run_pre_mart_checks_raises_with_all_failures(warehouse):
    make_stg_sales(warehouse, [
        ("P1", "2020-01-05", "2020-01-01", "100", "1", "DE", 5000.0, "ZZ", False, False, False, False),
    ])
    load_raw_rows(warehouse, [
        ("P1", "100", "Test Muni", "2020-01-05", "2020-01-01", "5000", "100", "1", "ZZ", "MYSTERY", "DE"),
    ])
    with pytest.raises(checks.CheckFailure) as exc_info:
        checks.run_pre_mart_checks(warehouse)
    names = {f["name"] for f in exc_info.value.failures}
    assert "unknown_sale_codes" in names
