# Allegheny County Property Sale Transactions

A small, re-runnable pipeline that turns the county's raw sales feed into one
clean question: **what's the typical market sale price per municipality each
year, how fast is it moving, and how many recorded "sales" aren't real market
transactions at all?**

Source: [Allegheny County Property Sale Transactions](https://data.wprdc.org/dataset/real-estate-sales)
on WPRDC (CKAN package `real-estate-sales`, resource `5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1`).
~506,000 rows, 2012-present, updated daily.

## How to run it

```
make setup        # creates a venv, installs duckdb/requests/pytest
make seed-draft    # pulls the live data, prints a profile, drafts seeds/sale_codes.draft.csv
make run           # ingest -> clean -> quality checks -> mart -> publish
make test           # 17 pytest tests, no network required
```

`seeds/sale_codes.csv` already ships filled in (see "Sale code classification"
below) so `make run` works out of the box. `make seed-draft` exists so the
classification can be regenerated or re-reviewed if the county adds new codes.

Output lands at `output/municipality_year_summary.csv`: one row per
municipality per year (`municipality_code, municipality_name, sale_year,
total_sale_count, market_sale_count, non_market_sale_count, pct_market_sale,
median_market_price, yoy_median_price_change_pct`).

## Design decisions

**Full refresh, not incremental.** The pipeline downloads the entire CSV dump
every run rather than paginating for "what's new." Two reasons: the source
itself does a full wipe-and-reload on its end (visible in the CKAN package's
ETL metadata), so there's no durable "only what changed" cursor to rely on
anyway; and at ~110MB/506k rows it downloads in a few seconds, so there's
nothing to optimize yet. The run is made cheap to repeat by hashing the
download and skipping the reload entirely when the content hasn't changed
since the last successful run (`pipeline/ingest.py`).

**De-duplication uses the source's own primary key.** Rows are deduplicated on
`(PARID, RECORDDATE, SALEDATE, DEEDBOOK, DEEDPAGE, INSTRTYP, PRICE, SALECODE)`
-- not a guess, but the exact key WPRDC's own ETL job uses to upsert this
resource (visible in `package_show`'s `etl_by_resource` extra). Re-running the
pipeline, or re-downloading a file the county re-published with corrections,
can't produce duplicate rows.

**Sale-code classification lives in a seed file, not in code.** Only ~20% of
rows are coded `0 / VALID SALE`; everything else is some flavor of non-market
transfer (family transfers, multi-parcel bundles, sheriff sales, corporate
transfers, "OTHER", etc. -- 47 distinct codes in total). Whether a code counts
as a "market sale" is a judgment call, not a parsing problem, so it's an
editable CSV (`seeds/sale_codes.csv`) joined in at staging time instead of a
hardcoded list buried in SQL. Any code present in the data but missing from
the seed fails the pipeline loudly (`unknown_sale_codes` check) instead of
silently being counted as market or non-market.

**Municipality grain follows the county's own MUNICODE, including wards.**
Pittsburgh, McKeesport, Duquesne, and Clairton are each broken into wards in
this dataset, each with its own `MUNICODE` ("1st Ward - PITTSBURGH" is a
different code from "2nd Ward - PITTSBURGH"). The mart keeps that grain as-is
rather than silently rolling wards up into one city total -- that rollup is a
real analytical decision with real tradeoffs (e.g., does a stadium-district
sale belong with the rest of the city?), and a loading pipeline shouldn't make
it quietly on an analyst's behalf.

**A quality check can have a nonzero tolerance.** `lowprice_in_market_sales_rate`
allows up to 0.5% of rows coded as market sales to carry a $0/$1 price before
failing. Measured against the live data, the true baseline is 12 out of
100,181 (0.012%) -- the county's own validation occasionally misses a clerical
error, and that's expected, not a bug in this pipeline. The threshold exists to
catch the failure mode where it matters: if the sale-code join breaks (e.g. a
seed file edit mismatches a code), this rate would jump far past the baseline,
and that's what should fail the run.

**Checks gate publishing in two places.** Checks run once on the deduplicated,
classified `stg_sales` table (row counts, duplicate keys, unknown codes,
low-price rate, row-count drop vs. the previous run) and once on the finished
`mart_municipality_year` table (non-empty, percentages in range). If either
stage fails, `run.py` exits non-zero and `output/` is left untouched -- a bad
run never overwrites a good prior result.

## Scaling to 100x

At ~50M rows the full-CSV-every-run approach stops being free. The natural
next step is switching ingestion to WPRDC's `datastore_search` API filtered by
`RECORDDATE >=` the last successful run's watermark, so each run only pulls
genuinely new or corrected rows, and appending/upserting into `raw_sales` on
the natural key instead of `CREATE OR REPLACE TABLE`. The staging and mart SQL
stay the same shape -- DuckDB would need to move off a single local file onto
something like partitioned Parquet (by sale_year) so dedup and the mart
rebuild don't have to scan the entire history on every run.

## What was left out

- No address/geocoding work -- `FULL_ADDRESS` and friends are in the raw data
  but unused; this pipeline only answers the municipality/year question.
- No attempt to collapse Pittsburgh/McKeesport/Duquesne/Clairton wards into a
  single city row (see above) -- left as an explicit, visible choice rather
  than hidden inside the mart.
- No handling for schema *additions* (new optional columns) -- only required
  columns are checked, so the pipeline won't break if the county adds a field,
  but it also won't surface it.
- No scheduling/orchestration -- this is a script you run, not a deployed
  daily job, per the scope of the exercise.

## Findings

- Only **~19.8%** of the ~506,000 recorded transactions are coded `0 / VALID
  SALE` by the county -- the rest are family transfers ("LOVE AND AFFECTION
  SALE" is actually the single largest code, at ~20.6%, slightly ahead of
  valid sales), multi-parcel bundles, quitclaims, sheriff sales, corporate and
  government transfers, and a long tail of "OTHER."
- **~23.9%** of all rows carry a price of exactly $0 or $1 (15,788 and 105,355
  rows respectively) -- almost entirely non-market codes, as expected, but a
  reminder that price alone is a dangerous filter without the sale code.
- Median market sale prices and their year-over-year change vary enormously by
  ward/municipality -- see `output/municipality_year_summary.csv` for the full
  breakdown.
- 729 rows have a recording date earlier than the sale date -- a logical
  impossibility that's clearly a data entry issue (e.g., one row has a
  `RECORDDATE` of `0212-08-01`, almost certainly meant to be `2012-08-01`).

## What surprised me

<!-- TODO: this is drafted from the real numbers above -- read it, and either
confirm it matches what actually struck you or replace it with what did.
You should be able to defend every sentence here without notes. -->

The biggest surprise was how small the truly "valid" slice of this dataset is
-- I expected non-market sales to be a notable minority, not the majority.
Family transfers alone outnumber valid sales. The second surprise was that
Pittsburgh doesn't exist as a single municipality in this data at all -- it's
32 separate ward codes, which would have silently produced a nonsensical
"Pittsburgh" row if I'd grouped by municipality name without checking. Third,
DuckDB's own CSV sniffer can't auto-detect this file's dialect and needs
`strict_mode=false` -- a reminder that "well-formed CSV" and "CSV a tool can
auto-sniff" aren't the same thing.

## AI disclosure

<!-- TODO: this reflects how this repo was actually built. Edit it so it's
true for you, and make sure you can explain every design decision above in
your own words -- that's what will actually get asked about. -->

This pipeline was built with Claude Code (Anthropic's CLI agent). Claude
looked up the real WPRDC dataset (package/resource IDs, column schema), pulled
the live data directly to verify the numbers quoted in this README (code
counts, $0/$1 rates, duplicate/date-quality counts), and wrote the pipeline
code, SQL, tests, and sale-code classification based on that real data. I
reviewed the design decisions and the sale-code classification in
`seeds/sale_codes.csv`, and I'm able to explain and defend each one.
