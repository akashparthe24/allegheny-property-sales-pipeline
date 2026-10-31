"""Builds property_sales_clean (cleaned/flagged) and property_sales_mart (the
final answer table: one row per municipality per year) from property_sales_load.

Clean-table rules: drop unusable rows (PARID/SALEDATE/SALECODE/INSTRTYP/MUNICODE
blank, SALEDATE unparseable, PRICE negative); cast types and trim text, but keep
the row; flag anomalies ($0/$1 price, record-before-sale date, bad zip/state)
rather than dropping them.

Two gotchas _blank() works around: SQLite's CAST('' AS REAL) silently returns
0.0 not NULL, and "blank" in this data is sometimes NULL and sometimes '' (not
just one or the other) -- a plain `= ''` check misses true NULLs.

Mart table: "market sale" comes from SALE_CODES below, a human-reviewed judgment
call per code (kept as Python, not a CSV, since the database itself is
gitignored and this is the only non-derived data in the project -- it needs a
version-controlled home). SQLite has no MEDIAN aggregate, so build_mart_table()
computes it by hand via ranked window functions.
"""
from . import config

# SQL fragment: true if `col` is NULL or ''.
def _blank(col: str) -> str:
    return f'("{col}" IS NULL OR "{col}" = \'\')'


CREATE_CLEAN_SQL = f"""
DROP TABLE IF EXISTS property_sales_clean;

CREATE TABLE property_sales_clean AS
SELECT
    "PARID"                                                                  AS "PARID",
    TRIM("MUNIDESC")                                                         AS "MUNIDESC",
    "MUNICODE"                                                               AS "MUNICODE",
    date("SALEDATE")                                                         AS "SALEDATE",
    CASE WHEN {_blank("RECORDDATE")} THEN NULL ELSE date("RECORDDATE") END   AS "RECORDDATE",
    CASE WHEN {_blank("PRICE")} THEN NULL ELSE CAST("PRICE" AS REAL) END     AS "PRICE",
    "DEEDBOOK"                                                               AS "DEEDBOOK",
    "DEEDPAGE"                                                               AS "DEEDPAGE",
    "SALECODE"                                                               AS "SALECODE",
    "SALEDESC"                                                               AS "SALEDESC",
    "INSTRTYP"                                                               AS "INSTRTYP",
    "PROPERTYZIP"                                                            AS "PROPERTYZIP",
    "PROPERTYSTATE"                                                          AS "PROPERTYSTATE",
    CASE WHEN NOT {_blank("PRICE")} AND CAST("PRICE" AS REAL) IN (0, 1)
         THEN 1 ELSE 0 END                                                   AS "IS_ZERO_OR_ONE_DOLLAR",
    CASE WHEN NOT {_blank("RECORDDATE")} AND date("RECORDDATE") < date("SALEDATE")
         THEN 1 ELSE 0 END                                                   AS "RECORDED_BEFORE_SALE",
    CASE WHEN {_blank("PROPERTYZIP")} OR LENGTH("PROPERTYZIP") != 5
         THEN 1 ELSE 0 END                                                   AS "IS_ZIP_INVALID",
    CASE WHEN {_blank("PROPERTYSTATE")} OR "PROPERTYSTATE" != 'PA'
         THEN 1 ELSE 0 END                                                   AS "IS_STATE_NOT_PA"
FROM {config.LOAD_TABLE}
WHERE NOT {_blank("PARID")}
  AND NOT {_blank("SALEDATE")} AND date("SALEDATE") IS NOT NULL
  AND NOT {_blank("SALECODE")}
  AND NOT {_blank("INSTRTYP")}
  AND NOT {_blank("MUNICODE")}
  AND ({_blank("PRICE")} OR CAST("PRICE" AS REAL) >= 0);
"""


# Rebuild property_sales_clean from property_sales_load (full rebuild, not
# incremental). Returns the new row count.
def build_clean_table(con) -> int:
    con.executescript(CREATE_CLEAN_SQL)
    return con.execute("SELECT COUNT(*) FROM property_sales_clean").fetchone()[0]


# (sale_code, sale_desc, is_market_sale, notes) -- one row per distinct SALECODE
# found in the live data, reviewed by hand.
SALE_CODES = [
    ("0", "VALID SALE", True, "County's own valid flag -- the baseline arm's-length market sale."),
    ("PV", "CLEAN AND GREEN VALID PREFERENTIAL", True, "Preferential-assessment sale the county itself still marks VALID."),
    ("3", "LOVE AND AFFECTION SALE", False, "Family/gift transfer; price is not a market price by definition."),
    ("H", "MULTI-PARCEL SALE", False, "Price covers multiple parcels together, not this one parcel alone."),
    ("14", "TIME ON MARKET (INSUFF/EXCESS)", False, "County flags the exposure period as abnormal, so price isn't a clean comp."),
    ("36", "QUIT CLAIM", False, "Quitclaim deeds are typically used to clear title or transfer within family, not arm's-length sales."),
    ("9", "OTHER INVALID SALES INDICATED", False, "Explicitly labeled invalid by the county."),
    ("99", "CORRECTIVE DEED / DUPLICATE SALE", False, "Administrative correction or duplicate record, not a new transaction."),
    ("AA", "SALE NOT ANALYZED", False, "County has not reviewed or confirmed validity -- treated as not-confirmed-market rather than assumed valid."),
    ("1", "SHERIFF SALE", False, "Foreclosure auction, not an open-market sale."),
    ("UR", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("35", "CORPORATION TRANSFER", False, "Internal corporate restructuring transfer, not a market transaction."),
    ("GV", "GOVERNMENT SALE", False, "Government party to the sale; not open-market pricing."),
    ("16", "BUILDING NOT YET ASSESSED", False, "Assessment-status flag, not a confirmed market price."),
    ("DT", "DATE OF TRANSFER (RECORD YR <> SALE YR)", False, "Administrative timing flag, not a confirmation of market validity."),
    ("Z", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("27", "NO ASSESSED VALUATION", False, "Assessment-status flag, not a confirmed market price."),
    ("2", "CITY TREASURER SALE", False, "Tax or treasurer sale, not open-market."),
    ("37", "ESTATE SALE", False, "Estate transfers are frequently not arm's-length market prices."),
    ("N", "NOT APPLICABLE", False, "Explicitly not applicable."),
    ("33", "SALE OF PREVIOUS FORECLOSURE - INVALID", False, "Explicitly labeled invalid."),
    ("U", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("BK", "BANK/FINANCIAL INSTITUTION", False, "Bank-owned or REO disposition, not a typical open-market sale."),
    ("13", "EXEMPT BUYER OR SELLER", False, "Exempt party to the transaction; price basis is not necessarily market-driven."),
    ("28", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("ND", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("PA", "PREFERENTIAL ASMT", False, "Preferential-assessment program sale; unlike PV it is not flagged VALID by the county."),
    ("6", "CHANGED AFTER SALE", False, "Record altered after the fact; price validity is suspect."),
    ("19", "INCLUDES PERSONAL PROPERTY", False, "Price bundles non-real-property value, skewing it as a parcel comp."),
    ("5", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("34", "SHORT SALE - INVALID", False, "Explicitly labeled invalid."),
    ("32", "COURT ORDERED SALE", False, "Court-mandated, not an open-market sale."),
    ("X", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("29", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("VB", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("V", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("0X", "OTHER", False, "Visually similar to code 0, but is a distinct code -- not assumed to mean the same thing."),
    ("XX", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("S", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("4", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("8", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("50", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("UX", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("VH", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("21", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("PX", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
    ("YY", "OTHER", False, "Unspecified other catch-all; no basis to confirm market validity."),
]


# Rebuild sale_codes_seed from SALE_CODES. Call before build_mart_table().
def load_sale_codes_seed(con):
    con.execute("DROP TABLE IF EXISTS sale_codes_seed")
    con.execute(
        'CREATE TABLE sale_codes_seed ("SALE_CODE" TEXT, "SALE_DESC" TEXT, "IS_MARKET_SALE" INTEGER, "NOTES" TEXT)'
    )
    rows = [(code, desc, 1 if is_market else 0, notes) for code, desc, is_market, notes in SALE_CODES]
    con.executemany("INSERT INTO sale_codes_seed VALUES (?, ?, ?, ?)", rows)
    return len(rows)


CREATE_MART_SQL = """
DROP TABLE IF EXISTS property_sales_mart;

CREATE TABLE property_sales_mart AS
WITH classified AS (
    SELECT
        c."MUNICODE",
        c."MUNIDESC",
        CAST(strftime('%Y', c."SALEDATE") AS INTEGER) AS "SALE_YEAR",
        c."PRICE",
        COALESCE(s."IS_MARKET_SALE", 0) AS "IS_MARKET_SALE",
        (s."SALE_CODE" IS NOT NULL) AS "IS_KNOWN_CODE"
    FROM property_sales_clean c
    LEFT JOIN sale_codes_seed s ON s."SALE_CODE" = c."SALECODE"
),
ranked_prices AS (
    SELECT
        "MUNICODE", "SALE_YEAR", "PRICE",
        ROW_NUMBER() OVER (PARTITION BY "MUNICODE", "SALE_YEAR" ORDER BY "PRICE") AS rn,
        COUNT(*) OVER (PARTITION BY "MUNICODE", "SALE_YEAR") AS cnt
    FROM classified
    WHERE "IS_MARKET_SALE" = 1 AND "PRICE" IS NOT NULL AND "PRICE" > 1
),
median_prices AS (
    SELECT "MUNICODE", "SALE_YEAR", AVG("PRICE") AS "MEDIAN_MARKET_PRICE"
    FROM ranked_prices
    WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2)
    GROUP BY "MUNICODE", "SALE_YEAR"
),
yearly AS (
    SELECT
        "MUNICODE", "MUNIDESC", "SALE_YEAR",
        COUNT(*) AS "TOTAL_SALE_COUNT",
        SUM("IS_MARKET_SALE") AS "MARKET_SALE_COUNT",
        SUM(CASE WHEN "IS_MARKET_SALE" = 0 THEN 1 ELSE 0 END) AS "NON_MARKET_SALE_COUNT"
    FROM classified
    GROUP BY "MUNICODE", "MUNIDESC", "SALE_YEAR"
)
SELECT
    y."MUNICODE", y."MUNIDESC", y."SALE_YEAR",
    y."TOTAL_SALE_COUNT", y."MARKET_SALE_COUNT", y."NON_MARKET_SALE_COUNT",
    ROUND(100.0 * y."MARKET_SALE_COUNT" / y."TOTAL_SALE_COUNT", 1) AS "PCT_MARKET_SALE",
    m."MEDIAN_MARKET_PRICE",
    ROUND(
        100.0 * (m."MEDIAN_MARKET_PRICE" - LAG(m."MEDIAN_MARKET_PRICE") OVER (
            PARTITION BY y."MUNICODE" ORDER BY y."SALE_YEAR"
        )) / LAG(m."MEDIAN_MARKET_PRICE") OVER (
            PARTITION BY y."MUNICODE" ORDER BY y."SALE_YEAR"
        ), 1
    ) AS "YOY_MEDIAN_PRICE_CHANGE_PCT"
FROM yearly y
LEFT JOIN median_prices m ON m."MUNICODE" = y."MUNICODE" AND m."SALE_YEAR" = y."SALE_YEAR"
ORDER BY y."MUNIDESC", y."SALE_YEAR";
"""


# Rebuild property_sales_mart (the final output table) from
# property_sales_clean + sale_codes_seed. Returns the new row count.
def build_mart_table(con) -> int:
    con.executescript(CREATE_MART_SQL)
    return con.execute("SELECT COUNT(*) FROM property_sales_mart").fetchone()[0]
