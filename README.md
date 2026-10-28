# Allegheny County Property Sale Transactions

A small, re-runnable pipeline that turns the county's raw sales feed into one
clean question: **what's the typical market sale price per municipality each
year, how fast is it moving, and how many recorded "sales" aren't real market
transactions at all?**

Source: [Allegheny County Property Sale Transactions](https://data.wprdc.org/dataset/real-estate-sales)
on WPRDC (CKAN package `real-estate-sales`, resource `5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1`).
~398,000 rows as of this submission, 2012-present, updated by the county on its
own schedule (see "What surprised me" below -- that number moved a lot during
the week this was built).

## How to run it

```
make setup        # creates a venv, installs duckdb/requests/pytest
make seed-draft    # pulls the live data, prints a profile, drafts seeds/sale_codes.draft.csv
make run           # ingest -> clean -> quality checks -> mart -> publish
make test           # 19 pytest tests, no network required
```

`seeds/sale_codes.csv` already ships filled in (see design decision 3, below)
so `make run` works out of the box. `make seed-draft` exists so the
classification can be regenerated or re-reviewed if the county adds new codes.

Output lands at `output/municipality_year_summary.csv`: one row per
municipality per year (`municipality_code, municipality_name, sale_year,
total_sale_count, market_sale_count, non_market_sale_count, pct_market_sale,
median_market_price, yoy_median_price_change_pct`).

Every run is tracked in two DuckDB tables, independent of the output table --
see "Run and error tracking" below.

## Design decisions

These are the four decisions I'd defend as the most consequential. Each one
had a real alternative I considered and rejected.

**1. Full refresh every run, not incremental pagination.**
I considered pulling only new/changed rows via `datastore_search` with a
`RECORDDATE >=` cursor, which is the obvious move for a dataset that's
"updated daily." I rejected it for now because the source itself does a full
wipe-and-reload on its end (visible in the CKAN package's own ETL metadata),
so there's no durable "only what changed" cursor to anchor an incremental
pull against -- and at ~85-110MB/400-500k rows, downloading the whole file
takes a few seconds, so there's nothing to optimize yet. I made the repeat
cost of full refresh cheap instead: the download is hashed, and the reload is
skipped entirely if the content hasn't changed since the last successful run
(`pipeline/ingest.py`). This is the first thing I'd revisit at 100x scale --
see below.

**2. De-duplication on the source's own natural key, not a surrogate key.**
The alternative was to let DuckDB assign a row number or hash the full row as
a synthetic primary key. I rejected that because it would make "is this the
same sale as last time" an accident of column order rather than a real
business rule. Instead, rows are deduplicated on
`(PARID, RECORDDATE, SALEDATE, DEEDBOOK, DEEDPAGE, INSTRTYP, PRICE, SALECODE)`
-- the exact key WPRDC's own ETL job uses to upsert this resource (visible in
`package_show`'s `etl_by_resource` extra). Re-running the pipeline, or
re-downloading a file the county re-published with corrections, can't produce
duplicate rows, because the key isn't a guess.

**3. Sale-code classification lives in an editable seed file, not in code.**
The alternative was a `CASE WHEN sale_code IN (...)` block baked into the SQL.
I rejected that because whether a code counts as a "market sale" is a judgment
call about 47 different codes (only ~17% of rows are the plain `0 / VALID
SALE` code), not a parsing problem -- burying that judgment in SQL would make
it invisible and hard to review. It's an editable CSV (`seeds/sale_codes.csv`)
with a reason recorded for every code, joined in at staging time. Any code
present in the data but missing from the seed fails the pipeline loudly
(`unknown_sale_codes` check) instead of silently defaulting to market or
non-market.

**4. A full run/error audit trail in DuckDB, not print-only logging.**
The first version of this pipeline only printed check results to stdout and
exited non-zero on failure -- nothing was queryable after the terminal closed.
I rejected leaving it that way once I actually hit a real failure while
building this (see below) and had no way to look back at what happened
without re-reading scrollback. Now every run writes a row to `pipeline_runs`
(started, finished, status, which stage failed if any, row counts at every
stage, output path, headline stats) and every error or failed check writes a
row to `pipeline_errors` (stage, error type, message), keyed by the same
`run_id`. A run that trips three checks at once gets three queryable error
rows, not one swallowed exception.

## Run and error tracking

Two DuckDB tables, inserted by `run.py`, independent of `load_log` (which only
tracks ingest download/skip decisions):

- **`pipeline_runs`** -- one row per execution. Inserted as `status='running'`
  at the start, then updated in place: either to `'success'` with
  `raw_row_count`, `staged_row_count`, `mart_row_count`, `output_path`, and
  two headline stats (`pct_market_sale_overall`, `median_market_price_overall`),
  or to `'failed'` with `failed_stage` naming exactly which stage broke
  (`ingest` / `staging` / `pre_checks` / `mart` / `post_checks` / `publish`).
- **`pipeline_errors`** -- one row per error *event*, foreign-keyed to
  `pipeline_runs.run_id`. A failed quality check logs its own name and detail
  message here; an exception logs its type and message. Nothing is published
  if a run fails, but the failure is still fully queryable afterward.

Both tables are append-only history, not something you read by eyeballing a
terminal -- e.g. `SELECT * FROM pipeline_errors WHERE error_type =
'unknown_sale_codes' ORDER BY occurred_at DESC` answers "how often has this
specific check failed" without re-running anything.

## Scaling to 100x / daily refresh

At ~40-50M rows the full-CSV-every-run approach stops being free, and it's
the first thing I'd change: switch ingestion to `datastore_search` filtered by
a watermark on `RECORDDATE`, and upsert into `raw_sales` on the natural key
instead of `CREATE OR REPLACE TABLE`. The staging and mart SQL stay the same
shape, but DuckDB on a single local file would need to move to partitioned
Parquet (by `sale_year`) so dedup and the mart rebuild don't re-scan the
entire history every run. For a daily unattended refresh, the pipeline logic
itself doesn't need to change -- it's already idempotent and it won't publish
on a failed check -- what's missing is an actual scheduler (cron or an
orchestrator like Airflow/Dagster calling `make run`) and alerting on top of
`pipeline_runs`/`pipeline_errors` (today a failure just sits in the table
until someone queries it; at scale that should page someone, and a run that
needed a manual override like `ALLOW_ROW_COUNT_DROP` absolutely should).

## What was left out

- No address/geocoding work -- `FULL_ADDRESS` and friends are in the raw data
  but unused; this pipeline only answers the municipality/year question.
- No attempt to collapse Pittsburgh/McKeesport/Duquesne/Clairton's ward-level
  `MUNICODE`s into one city row. The mart keeps the county's own grain as-is
  rather than silently rolling wards up -- that's a real analytical decision
  with real tradeoffs (does a stadium-district sale belong with the rest of
  the city?), and a loading pipeline shouldn't make it quietly on an analyst's
  behalf.
- No alerting on a failed or overridden run -- failures and overrides are
  fully logged in `pipeline_runs`/`pipeline_errors`, but nothing notifies a
  human; today you have to think to query it.
- No handling for schema *additions* (new optional columns) -- only required
  columns are checked, so the pipeline won't break if the county adds a
  field, but it also won't surface it.
- No scheduling/orchestration -- this is a script you run, not a deployed
  daily job, per the scope of the exercise.

## Findings

- Only **17.0%** of the ~398,000 recorded transactions are coded `0 / VALID
  SALE` by the county -- the single largest category is actually family
  transfers ("LOVE AND AFFECTION SALE", 19.9%), ahead of valid sales, followed
  by multi-parcel bundles, quitclaims, and a long tail of other non-market
  codes.
- **23.1%** of all rows carry a price of exactly $0 or $1 (13,325 and 78,717
  rows respectively) -- almost entirely non-market codes, as expected, but a
  reminder that price alone is a dangerous filter without the sale code.
- Median market sale price and its year-over-year change vary enormously
  across the county's 175 distinct municipality/ward codes -- see
  `output/municipality_year_summary.csv` for the full breakdown.
- 713 rows have a recording date earlier than the sale date -- a logical
  impossibility that's clearly a data-entry issue (e.g., one row has a
  `RECORDDATE` of `0212-08-01`, almost certainly meant to be `2012-08-01`).

## What surprised me

The real surprise happened mid-build, not in a single static pull: on October
27th, `make run` failed a check it had never failed before --
`row_count_vs_previous_run` -- reporting a 21.4% drop, from 506,315 rows to
397,885. My first assumption was a broken or partial download, which is
exactly the failure mode that check exists to catch. I re-ran it, got the
identical file back (same hash), and then went around the pipeline entirely
and hit WPRDC's `datastore_search` API directly: its own `total` field
reported 397,885, and the resource's `last_modified` timestamp showed
2026-10-26 -- the day before I ran this. The county had genuinely reloaded
the dataset with 108,430 fewer rows. That's not a bug in the
file, it's a real change to the source, and it's exactly the scenario the
exercise's "explain how you'd handle an update" requirement was asking about
-- I just got to live it instead of hypothesizing about it. I ended up adding
an explicit, logged override (`ALLOW_ROW_COUNT_DROP=1`) rather than loosening
the check's threshold, because a permanently looser threshold would have
blinded the pipeline to the next *actual* partial download.

Two smaller surprises, from the data itself: Pittsburgh doesn't exist as a
single municipality in this dataset at all -- it's dozens of separate ward
codes, which would have silently produced a nonsensical "Pittsburgh" row if
I'd grouped by municipality name without checking first. And DuckDB's own CSV
sniffer can't auto-detect this file's dialect -- it errors out of the box and
needs `strict_mode=false` -- a reminder that "well-formed CSV" and "CSV a
tool can auto-sniff" aren't the same thing.

## AI disclosure

I built this with Claude Code (Anthropic's CLI agent) as my primary tool
through the whole exercise, not just for a narrow sub-task. Claude looked up
the real WPRDC dataset (package and resource IDs, column schema), pulled the
live data to profile it and draft the sale-code classification, and wrote the
pipeline code, SQL, and tests against that real data rather than synthetic
fixtures. When the `row_count_vs_previous_run` check failed against live data
during the build (see above), Claude helped me investigate it -- querying
WPRDC's `datastore_search` API directly to confirm the drop was real rather
than a download bug -- and I made the call on how to handle it (an explicit,
audited override, not a loosened threshold). I reviewed every design
decision above, the sale-code classification in `seeds/sale_codes.csv`, and
the quality-check logic, and I can explain and defend each one without
notes.
