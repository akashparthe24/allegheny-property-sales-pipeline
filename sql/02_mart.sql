-- One row per municipality per year: the actual answer to the question this
-- pipeline exists to answer -- what's the typical market sale price, how fast is
-- it moving, and what share of recorded "sales" are not real market transactions.
--
-- Note: in this dataset, Pittsburgh, McKeesport, Duquesne, and Clairton are each
-- broken into wards with their own MUNICODE (e.g. "1st Ward - PITTSBURGH" is a
-- distinct municipality_code from "2nd Ward - PITTSBURGH"). We keep that grain as
-- the county defines it rather than silently re-aggregating wards up into one city
-- row -- collapsing them is a real analytical decision with real tradeoffs, not
-- something a loading pipeline should decide quietly on your behalf.

CREATE OR REPLACE TABLE mart_municipality_year AS
WITH yearly AS (
    SELECT
        municipality_code,
        municipality_name,
        sale_year,
        COUNT(*)                                                   AS total_sale_count,
        SUM(CASE WHEN is_market_sale THEN 1 ELSE 0 END)             AS market_sale_count,
        SUM(CASE WHEN NOT is_market_sale THEN 1 ELSE 0 END)         AS non_market_sale_count,
        MEDIAN(CASE WHEN is_market_sale AND price > 1 THEN price END) AS median_market_price
    FROM stg_sales
    GROUP BY municipality_code, municipality_name, sale_year
)
SELECT
    municipality_code,
    municipality_name,
    sale_year,
    total_sale_count,
    market_sale_count,
    non_market_sale_count,
    ROUND(100.0 * market_sale_count / total_sale_count, 1)          AS pct_market_sale,
    median_market_price,
    ROUND(
        100.0 * (median_market_price - LAG(median_market_price) OVER (
            PARTITION BY municipality_code ORDER BY sale_year
        )) / LAG(median_market_price) OVER (
            PARTITION BY municipality_code ORDER BY sale_year
        ),
        1
    )                                                                AS yoy_median_price_change_pct
FROM yearly
ORDER BY municipality_name, sale_year;
