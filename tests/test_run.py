import pytest

from pipeline import config, db, ingest
import run as run_module
from conftest import write_seed, DEFAULT_SEED_ROWS

HEADER = "PARID,MUNICODE,MUNIDESC,RECORDDATE,SALEDATE,PRICE,DEEDBOOK,DEEDPAGE,SALECODE,SALEDESC,INSTRTYP\n"
GOOD_ROW = "P1,100,Test Muni,2020-01-05,2020-01-01,100000,100,1,0,VALID SALE,DE\n"
GOOD_CONTENT = (HEADER + GOOD_ROW).encode()


def _run(warehouse, monkeypatch, tmp_path, download_content):
    seed_path = tmp_path / "sale_codes.csv"
    write_seed(seed_path, DEFAULT_SEED_ROWS)
    monkeypatch.setattr(config, "SEED_PATH", seed_path)
    monkeypatch.setattr(ingest, "_download", lambda: download_content)
    monkeypatch.setattr(db, "connect", lambda: warehouse)
    return run_module.main()


def test_successful_run_is_logged_as_success_with_summary(warehouse, monkeypatch, tmp_path):
    _run(warehouse, monkeypatch, tmp_path, GOOD_CONTENT)

    row = warehouse.execute(
        "SELECT status, failed_stage, raw_row_count, staged_row_count, mart_row_count, "
        "output_path, pct_market_sale_overall FROM pipeline_runs ORDER BY run_id DESC LIMIT 1"
    ).fetchone()
    status, failed_stage, raw_n, staged_n, mart_n, output_path, pct_market = row

    assert status == "success"
    assert failed_stage is None
    assert raw_n == 1 and staged_n == 1 and mart_n == 1
    assert output_path == str(config.OUTPUT_PATH)
    assert pct_market == 100.0
    assert warehouse.execute("SELECT COUNT(*) FROM pipeline_errors").fetchone()[0] == 0


def test_failed_check_is_logged_to_both_run_and_error_tables(warehouse, monkeypatch, tmp_path):
    bad_row = "P1,100,Test Muni,2020-01-05,2020-01-01,100000,100,1,ZZ,MYSTERY,DE\n"
    bad_content = (HEADER + bad_row).encode()  # "ZZ" is not in the seed -> unknown_sale_codes check fails

    with pytest.raises(SystemExit):
        _run(warehouse, monkeypatch, tmp_path, bad_content)

    status, failed_stage = warehouse.execute(
        "SELECT status, failed_stage FROM pipeline_runs ORDER BY run_id DESC LIMIT 1"
    ).fetchone()
    assert status == "failed"
    assert failed_stage == "pre_checks"

    error_name = warehouse.execute(
        "SELECT error_type FROM pipeline_errors ORDER BY error_id DESC LIMIT 1"
    ).fetchone()[0]
    assert error_name == "unknown_sale_codes"
