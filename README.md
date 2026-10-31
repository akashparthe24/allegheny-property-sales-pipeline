# Allegheny County Property Sale Transactions

A small, re-runnable pipeline that turns the county's live property sale feed
into one clean answer: **what's the typical market sale price per
municipality each year, how fast is it moving, and how many recorded "sales"
are not real market transactions at all?**

Source: [Allegheny County Property Sale Transactions](https://data.wprdc.org/dataset/real-estate-sales)
on WPRDC (CKAN resource `5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1`), pulled live via
the `datastore_search` API, 2012-present.

## How to run it

```
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python run.py
```

No downloads, no Docker, no cloud account -- everything lands in one local
SQLite file, `data/warehouse.sqlite3`.

Output lands at the `property_sales_mart` table inside that database: one row
per municipality per year (`MUNICODE, MUNIDESC, SALE_YEAR, TOTAL_SALE_COUNT,
MARKET_SALE_COUNT, NON_MARKET_SALE_COUNT, PCT_MARKET_SALE,
MEDIAN_MARKET_PRICE, YOY_MEDIAN_PRICE_CHANGE_PCT`). Inspect it with:

```
sqlite3 data/warehouse.sqlite3 "SELECT * FROM property_sales_mart LIMIT 10;"
```

**Why SQLite, over DuckDB/Postgres/dbt:** it needed zero setup (no Docker, no
server process, one file on disk), supports real `UNIQUE` indexes and
`ON CONFLICT ... DO UPDATE` upserts, which the re-runnability requirement
depends on directly, and ships with Python already. **Drawback:** it's
missing conveniences a warehouse-oriented engine like DuckDB has built in --
no `MEDIAN()` aggregate (computed by hand instead, via ranked window
functions), no native CSV reader, and single-writer only, which would matter
at real concurrency but doesn't here. I'd reach for Postgres or DuckDB instead
if this needed to serve concurrent readers/writers or handle the 100x-scale
data volumes discussed below.

## Design decisions

**1. Upsert on `(PARID, DEEDBOOK, DEEDPAGE)`, not a full reload every run.**
The alternative I rejected was what I actually built first: drop and reload
the whole raw table every run. It's simpler, but it throws away the question
"did anything actually change since last time," which matters once a pipeline
runs daily rather than once. The real key problem was picking the right
columns: a sale's identity can't include `PRICE`/`SALECODE`, because those are
exactly the fields the county corrects after the fact -- if they were part of
the key, a correction would look like "a new row appeared" instead of "an
existing row changed." I also rejected keying on `PARID` alone: a parcel can
legitimately sell many times (one parcel in this data has 24 separate sales
across 14 years), so that would have silently overwritten real transaction
history down to one row per property. The key landed on `(PARID, DEEDBOOK,
DEEDPAGE)` -- which document, which page, this specific transaction is
recorded on -- with a wider 8-column fallback key for the ~600 rows (0.1%)
missing deed info, since SQLite's unique index treats two blank values as
never equal to each other.
**Drawback:** two keys means two upsert statements and two partial indexes
instead of one simple `DROP`+reload -- more code to reason about, and it took
two real bugs (see "What surprised me") to get right. A full reload would have
been correct on day one; this needed testing against live data to trust.

**2. The clean layer flags anomalies; it doesn't drop them.**
The rejected alternative was silently excluding rows with a $0/$1 price or a
record-date-before-sale-date from the clean table. That's tempting because it
makes the "bad data" problem disappear, but it also means the published row
counts would quietly shrink with no record of why, and anyone reading the
output would have no way to know a filter had already been applied on their
behalf. Instead, `property_sales_clean` keeps every usable row and adds
boolean flag columns (`IS_ZERO_OR_ONE_DOLLAR`, `RECORDED_BEFORE_SALE`,
`IS_ZIP_INVALID`, `IS_STATE_NOT_PA`), so a $1 family gift is still counted as
a real transaction, just correctly excluded from the *market-price*
calculation specifically. Only rows that are genuinely unusable -- a blank
`PARID`, `SALEDATE`, `SALECODE`, `INSTRTYP`, `MUNICODE`, an unparseable sale
date, or a negative price -- are dropped, and that's a deliberate, documented
rule, not a silent side effect of a quality check.
**Drawback:** the clean table is not actually "clean" in the sense of being
safe to aggregate blindly -- anyone querying it has to know to filter on the
flag columns for a market-price calculation, or they'll silently include $1
gift sales in their numbers. A pre-filtered table would be harder to misuse,
at the cost of hiding information.

**3. Schema and row-count checks run *before* the real load, with an explicit,
logged override -- not a block with no way through.**
The rejected alternative was validating only after loading: pull everything,
then decide whether to trust it. I moved the two cheapest, highest-blast-radius
checks (does the API still have the columns we depend on, and did the total
row count move more than a normal day's activity would explain) to run against
just the first API page, before `property_sales_load` is touched at all. A
pure "block and refuse" design has a real failure mode, though: this county's
dataset does occasionally get reloaded with a genuinely different row count,
not because of a broken pipeline but because the source changed. A check with
no way past it would mean a legitimate change permanently wedges the pipeline.
So `ALLOW_ROW_COUNT_DROP=1` exists as a single-run, explicit override -- and
using it still writes a row to `pipeline_errors`, so an override leaves a
permanent trace instead of silently disabling the check.
**Drawback:** the override is still a human trusting their own judgment in
the moment -- it's logged, not prevented. A more rigorous version would cross-
check the drop against the source's own metadata (e.g. its `last_modified`
timestamp) automatically before allowing an override, rather than relying on
a person to have done that check themselves.

## Run and error tracking

Every execution of `run.py` writes a row to **`pipeline_runs`**: started as
`running`, updated at the end to `success` (with rows loaded and duration) or
`failed` (with `failed_stage` naming exactly which of the four stages broke:
`ingest`, `post_load_checks`, `transform_clean`, `transform_mart`). A process
that crashes mid-run leaves no permanent "stuck" state -- the next `connect()`
call automatically marks any leftover `running` row as `failed`/`orphaned`.

**`pipeline_errors`** holds one row per failure or quality-check violation,
keyed to `run_id`, so a run that trips three checks at once gets three
queryable rows, not one swallowed exception -- and an `ALLOW_ROW_COUNT_DROP`
override gets logged here too, even on an otherwise-successful run.

## Data quality checks

Twelve checks total, all of which genuinely abort the run on failure (not just
log a warning):

- **Pre-load** (run before any row is touched): required columns present;
  row count vs. the previous run, within 5% (either direction).
- **Post-load**: no duplicate keys under either the main or fallback upsert
  key; `PARID`/`SALEDATE`/`SALECODE`/`INSTRTYP`/`MUNICODE` never blank; `PRICE`
  never negative; `UPDATE_TIMESTAMP` never earlier than `INSERT_TIMESTAMP`;
  and the dataset-specific check below.

**The check written because of something found in this dataset:**
`RECORDDATE` before `SALEDATE` is a logical impossibility -- the deed can't be
recorded before the sale happened. It shows up on 730 rows (0.14%) in the live
data, almost certainly clerical typos (e.g. a year entered as `0212` instead
of `2012`). The check allows up to 1% before failing, calibrated to that
measured baseline -- a rate much higher than that would mean a real
date-parsing bug, not routine human error.

## Re-running / handling updates

Running `python run.py` twice back-to-back is safe: the upsert key means a
second run with unchanged source data touches zero rows (verified directly --
`INSERT_TIMESTAMP` and `UPDATE_TIMESTAMP` stayed identical across two
consecutive runs). When the source *does* change, new or corrected rows are
inserted/updated individually rather than requiring a full reload, and the
row-count check (see above) is what catches the source changing more
drastically than expected -- which, during this build, it genuinely did once:
a live pull came back with a 21% smaller row count than the pull three days
earlier. The check caught it correctly; I confirmed independently against the
API's own reported total that the smaller count was real, not a broken
download, before proceeding.

## Scaling to 100x / daily refreshes

At tens of millions of rows, paginating the full `datastore_search` endpoint
every run (currently ~100 HTTP requests, 1-3 minutes) stops being free. I'd
switch to pulling only rows past a watermark via `datastore_search_sql`
(confirmed this endpoint supports a real `WHERE` clause against this
resource), while keeping a periodic full-key-set reconciliation pass --
because a pure incremental-by-date pull can never notice a row *disappearing*
from the source, which is exactly the kind of change this project already
proved can happen. For daily unattended runs, the pipeline logic itself
doesn't need to change -- it's already idempotent and won't publish on a
failed check -- what's missing is an actual scheduler (cron, or an
orchestrator like Airflow/Dagster calling `run.py`) and alerting on top of
`pipeline_runs`/`pipeline_errors`, since today a failure or an override just
sits in a table until someone queries it.

## What was left out

- **No automated test suite.** Every piece of logic here was verified by
  hand against live data and deliberately-injected bad data during
  development (including catching two real bugs this way), but none of that
  is captured as a repeatable `pytest` suite in the repo. Given more time,
  this is the first thing I'd add back.
- **No address/geocoding work.** Address sub-fields are carried in the raw
  table but unused everywhere downstream -- this pipeline only answers the
  municipality/year question.
- **No CSV export.** The output lives as a real database table
  (`property_sales_mart`), which satisfies "produce one output table," but
  there's no step that also writes it to a flat file for easier sharing.
- **No alerting.** Failures and overrides are fully logged, but nothing
  notifies a human -- someone has to think to query `pipeline_errors`.
- **No scheduling/orchestration.** This is a script you run, not a deployed
  daily job, per the scope of the exercise.
- **No mart-level quality checks.** `property_sales_mart` itself isn't
  re-validated after being built (e.g. "does every row's percentage fall
  between 0 and 100"); only the raw and clean layers are checked.

## Findings

- Only **19.8%** of all 506,991 recorded transactions are genuine market
  sales by the county's own sale codes -- the rest are family transfers,
  sheriff sales, corporate transfers, multi-parcel bundles, and a long tail
  of administrative codes.
- **313,967** distinct parcels account for 506,991 rows -- most properties
  that appear more than once sold 2-3 times over the 2012-2026 window, but
  one parcel has 24 separate recorded sales.
- **600 rows** (0.1%) are missing deed book/page information and needed a
  separate matching rule to avoid duplicating on every run.
- **730 rows** (0.14%) have a recording date before the sale date -- a
  measurable, consistent baseline of clerical error in the source.

## What surprised me

The biggest surprise wasn't a property of the data itself -- it's that this
source has no stable size at all. During this build, a live pull came back
with 21% fewer rows than one pulled three days earlier, and I confirmed
independently (via the API's own `total` field and the resource's
`last_modified` timestamp) that it was a real change at the source, not a
broken download on my end. Any pipeline built against this feed has to assume
its *shape*, not just its contents, can change between runs.

The second surprise was more mundane but cost real debugging time: SQLite's
`CAST('' AS REAL)` silently returns `0.0`, not `NULL`. Combined with the fact
that "missing" in this API's response is sometimes a true JSON `null` and
sometimes an empty string -- not consistently one or the other -- a naive cast
of a blank `PRICE` would have miscast it to a literal $0 and then wrongly
flagged it as a suspiciously-cheap sale, conflating "we don't know the price"
with "the price really is zero." This was caught by testing the clean-table
build against real loaded data before trusting it (it silently dropped 2,711
otherwise-valid rows on the first attempt) rather than by reasoning about the
SQL alone.

## AI disclosure

I built this with Claude Code (Anthropic's CLI agent) as my primary tool
through the whole exercise. I directed the architecture through discussion --
the choice of SQLite, the upsert key design (and specifically rejecting
`PARID` alone and rejecting including `PRICE`/`SALECODE` in the key), the
flag-vs-drop rule for anomalies, and the check thresholds -- and Claude
implemented and tested each piece against the live dataset. We found and fixed
two real bugs together by testing against real data rather than trusting the
SQL in the abstract: a NULL-handling bug that silently excluded 2,711 valid
rows from the clean table, and a partial-index collision bug where normalizing
blank deed info for one upsert key unexpectedly collided with the other key's
index. I reviewed the design decisions, the sale-code classification, and the
quality-check logic above, and can explain and defend each one without notes.
