"""All settings in one place: source API, database path, upsert keys,
and quality-check thresholds."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

CKAN_BASE_URL = "https://data.wprdc.org"
RESOURCE_ID = "5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"  # Allegheny County Property Sale Transactions
DATASTORE_SEARCH_URL = f"{CKAN_BASE_URL}/api/3/action/datastore_search"

DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "warehouse.sqlite3"

LOAD_TABLE = "property_sales_load"
PAGE_SIZE = 5000

# Baseline rate of RECORDDATE-before-SALEDATE rows in this data is ~0.14%
# (clerical typos); allow some margin above that before failing.
MAX_RECORDED_BEFORE_SALE_PCT = 1.0  # percent

# Upsert key: which document/page a sale is recorded on. PRICE/SALECODE
# excluded on purpose -- those are exactly what the county corrects later.
MAIN_KEY = ["PARID", "DEEDBOOK", "DEEDPAGE"]

# Fallback key for the ~0.1% of rows with a blank DEEDBOOK/DEEDPAGE (NULL never
# equals NULL under a unique index, so those rows need a wider key instead).
# DEEDBOOK/DEEDPAGE are left out of the key itself -- they're always blank for
# every row this key applies to (see FALLBACK_INDEX_WHERE), so they can never
# discriminate between two fallback rows.
FALLBACK_KEY = ["PARID", "RECORDDATE", "SALEDATE", "INSTRTYP", "PRICE", "SALECODE"]

# Partial-index predicates -- each key only governs the rows it's meant to.
MAIN_INDEX_WHERE = '"DEEDBOOK" != \'\' AND "DEEDPAGE" != \'\''
FALLBACK_INDEX_WHERE = '"DEEDBOOK" = \'\' OR "DEEDPAGE" = \'\''
