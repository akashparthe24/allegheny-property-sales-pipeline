import pytest

from pipeline import ingest, config

HEADER = "PARID,MUNICODE,MUNIDESC,RECORDDATE,SALEDATE,PRICE,DEEDBOOK,DEEDPAGE,SALECODE,SALEDESC,INSTRTYP\n"
ROW_A = "P1,100,Test Muni,2020-01-05,2020-01-01,100000,100,1,0,VALID SALE,DE\n"
ROW_B = "P2,100,Test Muni,2021-02-05,2021-02-01,150000,101,1,0,VALID SALE,DE\n"

CONTENT_V1 = (HEADER + ROW_A).encode()
CONTENT_V2 = (HEADER + ROW_A + ROW_B).encode()  # "county published a correction"


def test_check_header_passes_with_required_columns():
    ingest._check_header(CONTENT_V1)  # should not raise


def test_check_header_fails_when_required_column_missing():
    bad = b"PARID,PRICE\nP1,100\n"
    with pytest.raises(ingest.SchemaChangedError):
        ingest._check_header(bad)


def test_rerunning_with_unchanged_data_does_not_duplicate(warehouse, monkeypatch):
    monkeypatch.setattr(ingest, "_download", lambda: CONTENT_V1)

    first = ingest.ingest(warehouse)
    second = ingest.ingest(warehouse)

    assert first["status"] == "downloaded"
    assert second["status"] == "skipped_unchanged"
    assert second["row_count"] == first["row_count"]
    row_count = warehouse.execute("SELECT COUNT(*) FROM raw_sales").fetchone()[0]
    assert row_count == 1  # not duplicated by re-running


def test_rerunning_with_new_data_replaces_rather_than_appends(warehouse, monkeypatch):
    monkeypatch.setattr(ingest, "_download", lambda: CONTENT_V1)
    ingest.ingest(warehouse)

    monkeypatch.setattr(ingest, "_download", lambda: CONTENT_V2)
    result = ingest.ingest(warehouse)

    assert result["status"] == "downloaded"
    row_count = warehouse.execute("SELECT COUNT(*) FROM raw_sales").fetchone()[0]
    assert row_count == 2  # matches the new file, not 1 (old) + 2 (new)


def test_raw_file_is_saved_untouched(warehouse, monkeypatch):
    monkeypatch.setattr(ingest, "_download", lambda: CONTENT_V1)
    result = ingest.ingest(warehouse)
    saved = config.RAW_DIR.glob("*.csv")
    assert any(p.read_bytes() == CONTENT_V1 for p in saved)
