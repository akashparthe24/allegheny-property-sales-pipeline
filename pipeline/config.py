"""All tunable settings in one place: API location, paths, schema expectations, thresholds."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# --- Source: WPRDC / Allegheny County CKAN instance ---
# Dataset: "Allegheny County Property Sale Transactions"
# https://data.wprdc.org/dataset/real-estate-sales
CKAN_BASE_URL = "https://data.wprdc.org"
PACKAGE_ID = "9e0ce87d-07b8-420c-a8aa-9de6104f61d6"
RESOURCE_ID = "5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"  # "Property Sales Transactions" (2012-present, ~506k rows, updated daily)
CSV_DUMP_URL = f"{CKAN_BASE_URL}/datastore/dump/{RESOURCE_ID}"

# --- Local paths ---
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "warehouse.duckdb"
OUTPUT_DIR = ROOT / "output"
OUTPUT_PATH = OUTPUT_DIR / "municipality_year_summary.csv"
SQL_DIR = ROOT / "sql"
SEED_PATH = ROOT / "seeds" / "sale_codes.csv"
DRAFT_SEED_PATH = ROOT / "seeds" / "sale_codes.draft.csv"

# --- Schema contract ---
# If any of these columns disappear from the source file, something upstream changed
# shape and we should stop rather than silently build a summary table out of garbage.
REQUIRED_COLUMNS = [
    "PARID", "MUNICODE", "MUNIDESC", "RECORDDATE", "SALEDATE",
    "PRICE", "DEEDBOOK", "DEEDPAGE", "SALECODE", "INSTRTYP",
]

# Business/natural key for a sale record. This mirrors the primary key WPRDC's own
# ETL uses to upsert this resource (see the package's `etl_by_resource` extra on
# https://data.wprdc.org/api/3/action/package_show?id=real-estate-sales), so it's not
# a guess -- it's the key the source system itself treats as uniquely identifying a sale.
NATURAL_KEY_COLUMNS = [
    "PARID", "RECORDDATE", "SALEDATE", "DEEDBOOK", "DEEDPAGE", "INSTRTYP", "PRICE", "SALECODE",
]

# --- Quality-check thresholds ---
# Measured against a live pull of the full dataset (2026-10-22): rows officially coded
# "0 / VALID SALE" that still carry a $0 or $1 price were 12 out of 100,148 (0.012%).
# A handful of these will always exist (clerical corrections), so we don't hard-fail on
# zero. We do fail if the rate jumps far past the observed baseline, since that would
# mean the sale-code join broke or a bad code got marked as a market sale.
MAX_LOWPRICE_IN_MARKET_SALES_PCT = 0.5  # percent

# A run that returns drastically fewer rows than the prior run likely means a partial
# or truncated download, not that the county deleted history.
MAX_ROW_COUNT_DROP_PCT = 5.0  # percent

# This check should default to failing loud, never to silently loosening its own
# threshold. The only way past it is an explicit, logged human override for a single
# run -- set after independently confirming the drop against the source (e.g. WPRDC's
# own datastore_search `total` and the resource's `last_modified`), not a config edit
# that would blind the check to the next *real* partial download.
ALLOW_ROW_COUNT_DROP_OVERRIDE = os.environ.get("ALLOW_ROW_COUNT_DROP") == "1"
