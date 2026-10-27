-- Build a clean, de-duplicated sales table from the raw CKAN dump.
--
-- Key decisions:
--   * SALEDATE is the date of record for "which year did this sale happen" -- it is
--     always populated in the source. RECORDDATE (when the deed was actually filed)
--     can lag the sale by years and is occasionally garbage (e.g. a typo'd year like
--     "0212-08-01"), so we TRY_CAST it and treat failures as NULL rather than
--     dropping the row -- a bad recording date doesn't mean the sale didn't happen.
--   * De-duplication uses the natural key WPRDC's own ETL uses to upsert this table
--     (PARID, RECORDDATE, SALEDATE, DEEDBOOK, DEEDPAGE, INSTRTYP, PRICE, SALECODE).
--     If the same key appears more than once (it can, across repeated downloads of a
--     dataset that gets corrected in place), we keep one row arbitrarily -- the rows
--     are identical on every field in the key, so there's nothing to pick between.
--   * is_market_sale comes from seeds/sale_codes.csv, a human-reviewed classification
--     of each SALECODE. Codes not present in the seed file flag is_known_code = false
--     instead of silently defaulting to one answer or the other -- see checks.py.

CREATE OR REPLACE TABLE stg_sales AS
WITH typed AS (
    SELECT
        r.PARID                                    AS parcel_id,
        TRIM(r.MUNIDESC)                            AS municipality_name,
        r.MUNICODE                                  AS municipality_code,
        TRY_CAST(r.SALEDATE AS DATE)                AS sale_date,
        TRY_CAST(r.RECORDDATE AS DATE)               AS record_date,
        TRY_CAST(r.PRICE AS DOUBLE)                  AS price,
        r.DEEDBOOK                                   AS deed_book,
        r.DEEDPAGE                                   AS deed_page,
        TRIM(r.SALECODE)                            AS sale_code,
        r.SALEDESC                                   AS sale_code_desc_source,
        r.INSTRTYP                                   AS instrument_type,
        -- natural key fields kept raw (pre-cast) for de-dup, matching upstream ETL pk
        r.PARID, r.RECORDDATE, r.SALEDATE, r.DEEDBOOK, r.DEEDPAGE, r.INSTRTYP, r.PRICE, r.SALECODE
    FROM raw_sales r
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY PARID, RECORDDATE, SALEDATE, DEEDBOOK, DEEDPAGE, INSTRTYP, PRICE, SALECODE
            ORDER BY parcel_id
        ) AS rn
    FROM typed
)
SELECT
    d.parcel_id,
    d.municipality_name,
    d.municipality_code,
    d.sale_date,
    YEAR(d.sale_date)                               AS sale_year,
    d.record_date,
    d.price,
    d.deed_book,
    d.deed_page,
    d.sale_code,
    d.sale_code_desc_source,
    d.instrument_type,
    COALESCE(s.is_market_sale, false)               AS is_market_sale,
    (s.sale_code IS NOT NULL)                       AS is_known_code,
    (d.price IS NOT NULL AND d.price <= 1)          AS is_zero_or_one_dollar,
    (d.record_date IS NOT NULL AND d.sale_date IS NOT NULL AND d.record_date < d.sale_date)
                                                     AS recorded_before_sale
FROM deduped d
LEFT JOIN sale_codes_seed s ON s.sale_code = d.sale_code
WHERE d.rn = 1
  AND d.sale_date IS NOT NULL;   -- a sale with no parseable date can't be assigned to a year
