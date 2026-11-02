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

Everything lands in SQLite file, `data/warehouse.sqlite3`.

Output lands at the `property_sales_mart` table inside that database: one row
per municipality per year (`MUNICODE, MUNIDESC, SALE_YEAR, TOTAL_SALE_COUNT,
MARKET_SALE_COUNT, NON_MARKET_SALE_COUNT, PCT_MARKET_SALE,
MEDIAN_MARKET_PRICE, YOY_MEDIAN_PRICE_CHANGE_PCT`). Inspect it with:

```
sqlite3 data/warehouse.sqlite3 "SELECT * FROM property_sales_mart LIMIT 10;"
```

Stack: Python and SQLite. No setup, no server process, real `UNIQUE` indexes
and `ON CONFLICT` upserts for re-runnability. At real concurrency or 100x
scale, Postgres or DuckDB would be the better choice.

## Design decisions

**1. Upsert on `(PARID, DEEDBOOK, DEEDPAGE)`, not a full reload every run.**
If a record is new, it gets inserted as a new transaction. If a record
already exists, we check it against three columns, `PARID`, `DEEDBOOK`, and
`DEEDPAGE`, and if those match an existing row, we update that row instead
of inserting a duplicate. To track this, we added two columns,
`INSERT_TIMESTAMP` and `UPDATE_TIMESTAMP`: a brand-new row gets both set to
the same value, an existing row that gets updated only has `UPDATE_TIMESTAMP`
refreshed, so we always know when a row first showed up versus when it last
changed, without a separate change log.

For about 600 records where `DEEDBOOK`/`DEEDPAGE` come back blank, that
three-column check can't work, a blank value doesn't reliably match another
blank value in SQL. So those rows go through a second key instead, built on
`PARID` plus a wider set of columns (`RECORDDATE`, `SALEDATE`, `INSTRTYP`,
`PRICE`, `SALECODE`), and get matched/updated against that condition instead.

The alternative I rejected was dropping and reloading the whole raw table
every run. It's simpler to write, but it throws away "did anything actually
change since last time" completely, every row looks brand new on every run,
which matters once this runs daily instead of once.
**Drawback:** running two different key checks instead of one plain reload
is genuinely more to maintain, and it wasn't free of risk either, I actually
broke it twice while building and later adjusting the second key: once from
a straight NULL-handling mistake, and once from trying to simplify that
second key from 8 columns down to 6, which quietly let all ~600 of those
rows start duplicating on rerun. A full reload would never have had either
problem. That's the real trade here, more correctness for more fragility,
and the fragility only showed up when I actually ran it against live data
twice, not while I was writing the SQL.

**2. The clean layer flags anomalies instead of dropping them.**
For `property_sales_clean`, we added four flag columns:
`IS_ZERO_OR_ONE_DOLLAR` (price is $0 or $1, usually a gift between family),
`RECORDED_BEFORE_SALE` (the deed was recorded before the sale happened,
which shouldn't be possible), `IS_ZIP_INVALID` (zip isn't 5 digits), and
`IS_STATE_NOT_PA` (state isn't PA). A row with any of these issues stays in
the table, just marked, so a $1 family transfer is still counted as a real
transaction, it's excluded only from the *market-price* calculation
specifically, not from the dataset. We only actually drop a row if it's
genuinely unusable: a blank `PARID`, `SALEDATE`, `SALECODE`, `INSTRTYP`, or
`MUNICODE`, a sale date that doesn't parse, or a negative price.

The alternative I rejected was silently excluding the $0/$1 and bad-date
rows from the clean table entirely. That makes the "bad data" problem
disappear, but it also means the published row count quietly shrinks with
no record of why, and anyone using the table has no way to know a filter
already ran behind their back.
**Drawback:** this means `property_sales_clean` isn't actually "clean" in
the sense that you can aggregate it blindly. Anyone using it has to know to
check the flag columns before computing a market price, or they'll silently
include $1 gift sales in their numbers. A table that was pre-filtered would
be harder to misuse by accident, at the cost of hiding that information from
whoever's using it.

**3. Validation happens entirely after loading, not split pre/post.**
What I actually did: every check, duplicate keys, not-null, price, dates,
timestamps, ten in total, runs after the data is already loaded into
`property_sales_load`, against the real table. I chose this because the
outcome I wanted was a check that catches an actual problem in the actual
data, not a guess made off the first page of an API response.

What I rejected: I'd originally built two checks to run before the real
load, a schema check (does the API still have every column we depend on)
and a row-count check (did the total move more than a normal day's activity
would explain). I pulled both out after actually living with them. The
row-count check needed a manual override (`ALLOW_ROW_COUNT_DROP=1`) just to
get through this source's normal volatility, one pull during this build
came back 21% smaller than one from three days earlier, and that was
legitimate, not a bug. A check that needs to be overridden regularly isn't
really protecting anything anymore. The schema check turned out to be
pointless for a different reason: if a column actually went missing from
the API, the pipeline would already fail loudly the moment anything tried
to reference that column by name, so the separate check wasn't catching
anything new.
**Drawback:** without a pre-load check, a genuinely bad pull still gets
fully written into `property_sales_load` before anything catches it, so a
broken run costs more wasted time and I/O than it would with a gate in
front. And if a column does go missing, the failure shows up as a generic
SQL error instead of a clear message naming exactly which column
disappeared.

## Transformations

Two stages, both in `pipeline/transform.py`:

**1. Raw → `property_sales_clean`**
- Drops only genuinely unusable rows: blank `PARID`/`SALEDATE`/`SALECODE`/
  `INSTRTYP`/`MUNICODE`, an unparseable `SALEDATE`, or a negative `PRICE`.
- Casts/cleans types on the rows it keeps: `PRICE` to a real number, dates to
  proper `date()` values, trims whitespace off `MUNIDESC`.
- Flags, doesn't drop, four anomalies as boolean columns instead of silently
  filtering: `IS_ZERO_OR_ONE_DOLLAR` ($0/$1 sales, usually gifts),
  `RECORDED_BEFORE_SALE` (the clerical-typo dates), `IS_ZIP_INVALID` (not 5
  digits), `IS_STATE_NOT_PA`.
- Full rebuild every run (not incremental), since it's cheap and derived
  entirely from the raw table.

**2. `property_sales_clean` → `property_sales_mart`**
- Joins each sale against a hand-reviewed `sale_codes_seed` table (47
  `SALECODE` values, each judged market vs. non-market, e.g. `0` = valid
  market sale, `1` = sheriff sale is not).
- Aggregates to one row per municipality per year: total sales, market
  sales, non-market sales, `PCT_MARKET_SALE`.
- Computes median market price by hand via `ROW_NUMBER()`/`COUNT() OVER`
  window functions (SQLite has no `MEDIAN()` aggregate), using only real
  market sales with `PRICE > 1`.
- Computes year-over-year median price change % via `LAG()` over each
  municipality's years.

In short: clean = filter + flag at the row level; mart = classify by sale
code, then aggregate to the municipality/year grain that actually answers
the stated question.

## Data quality checks

Ten checks total, all post-load (duplicate-key, not-null, non-negative-price,
and timestamp checks), all of which genuinely abort the run on failure, not
just log a warning. Every failure, and every run itself, is logged to two
tables (`pipeline_runs`, `pipeline_errors`), so a run that trips three checks
at once leaves three queryable rows, not one swallowed exception.

**The check written because of something found in this dataset:**
`RECORDDATE` before `SALEDATE` is a logical impossibility, since the deed
can't be recorded before the sale happened. It shows up on 730 rows (0.14%)
in the live data, almost certainly clerical typos (e.g. a year entered as
`0212` instead of `2012`). The check allows up to 1% before failing,
calibrated to that measured baseline. A rate much higher than that would
mean a real date-parsing bug, not routine human error.

## Re-running / handling updates

Running `python run.py` twice back-to-back is safe: the upsert key means a
second run with unchanged source data touches zero rows (verified directly:
`INSERT_TIMESTAMP` and `UPDATE_TIMESTAMP` stayed identical across two
consecutive runs). When the source *does* change, new or corrected rows are
inserted/updated individually rather than requiring a full reload. There's no
automatic guard against the source's total row count moving a lot between
runs (see decision #3), so a drastic shift wouldn't by itself stop a run. What
would still catch a genuinely broken pull is the post-load checks: a
corrupted or truncated download is far more likely to produce duplicate keys
or blank required columns than a clean, smaller-but-valid dataset is.

## Scaling to 100x / daily refreshes

At tens of millions of rows, paginating the full `datastore_search` endpoint
every run (currently ~100 HTTP requests, 1-3 minutes) stops being free. I'd
switch to pulling only rows past a watermark via `datastore_search_sql`
(confirmed this endpoint supports a real `WHERE` clause against this
resource), while keeping a periodic full-key-set reconciliation pass, because
a pure incremental-by-date pull can never notice a row *disappearing* from
the source, which is exactly the kind of change this project already proved
can happen. For daily unattended runs, the pipeline logic itself doesn't need
to change. It's already idempotent and won't publish on a failed check. What's
missing is an actual scheduler (cron, or an orchestrator like Airflow/Dagster
calling `run.py`) and alerting on top of `pipeline_runs`/`pipeline_errors`,
since today a failure or an override just sits in a table until someone
queries it.

## What I scoped out, and why

**Verification stayed manual, not automated into a `pytest` suite.** Every
rule in this pipeline, the upsert key, the clean-layer filters, all 10 quality
checks, was proven against live data and against deliberately-injected bad
rows during development, including catching three real bugs this way before
they could reach the output. That's the verification a reviewer actually
cares about. Turning it into a repeatable regression suite is the next thing
I'd build, not a gap in the logic itself.

**Everything outside the stated question was deliberately cut, not forgotten.**
Address and geocoding fields are carried in the raw table but untouched,
because the question this project answers is about price by municipality and
year, not location. A CSV export, a scheduler, and alerting on failed runs are
all straightforward additions once this moves from a one-off script to a
production job, which is a different phase of work than what this exercise
scopes. The one piece I'd add first at that point: a validation pass on
`property_sales_mart` itself (e.g. every percentage falls between 0 and 100),
mirroring the checks already on the raw and clean layers.

## Findings

From the pipeline build itself:

- Only **19.8%** of all 506,991 recorded transactions are genuine market
  sales by the county's own sale codes. The rest are family transfers,
  sheriff sales, corporate transfers, multi-parcel bundles, and a long tail
  of administrative codes.
- **313,967** distinct parcels account for 506,991 rows. Most properties
  that appear more than once sold 2-3 times over the 2012-2026 window, but
  one parcel has 24 separate recorded sales.
- **600 rows** (0.1%) are missing deed book/page information and needed a
  separate matching rule to avoid duplicating on every run.
- **730 rows** (0.14%) have a recording date before the sale date, a
  measurable, consistent baseline of clerical error in the source.

From actually querying `property_sales_mart` to answer the stated question:

- **The market-sale share roughly tripled starting in 2020.** It sat at
  7-16% from 2012-2019, then jumped to the high-20s/low-30s from 2020 onward
  and has stayed there through 2026. That's a structural shift, not noise,
  and anyone comparing pre-2020 and post-2020 numbers needs to account for it.
- **Median market price has roughly doubled since 2012** (~$130K to ~$289K,
  average across municipalities), with the steepest run-up in 2019-2022
  (the pandemic-era market), and has been flattening since (+5% in 2024,
  +4% in 2025).
- **There's an ~11x price gap across municipalities in the same year.**
  2025's highest medians were Edgeworth (~$999K) and Fox Chapel (~$977K);
  the lowest were 2nd Ward-Clairton (~$90K) and McKees Rocks (~$100K), each
  on 20+ market sales, so it's a real, persistent divide, not a thin-sample
  artifact.
- **`YOY_MEDIAN_PRICE_CHANGE_PCT` is noisy for small municipalities** and
  should be read with that in mind. Several municipalities with fewer than
  45 market sales in a year swung ±20-34% year over year on that count
  alone. The mart doesn't filter these out, since suppressing small
  municipalities would mean silently deciding what counts as a "real"
  market for someone else.

## What surprised me

The biggest surprise wasn't a property of the data itself. It's that this
source has no stable size at all. During this build, a live pull came back
with 21% fewer rows than one pulled three days earlier, and I confirmed
independently (via the API's own `total` field and the resource's
`last_modified` timestamp) that it was a real change at the source, not a
broken download on my end. Any pipeline built against this feed has to assume
its *shape*, not just its contents, can change between runs, which is exactly
why a simple row-count threshold (see decision #3) turned out to be the wrong
tool here.

The second surprise was more mundane but cost real debugging time. SQLite's
`CAST('' AS REAL)` silently returns `0.0`, not `NULL`. Combined with the fact
that "missing" in this API's response is sometimes a true JSON `null` and
sometimes an empty string, not consistently one or the other, a naive cast of
a blank `PRICE` would have miscast it to a literal $0 and then wrongly
flagged it as a suspiciously-cheap sale, conflating "we don't know the price"
with "the price really is zero." This was caught by testing the clean-table
build against real loaded data before trusting it (it silently dropped 2,711
otherwise-valid rows on the first attempt), rather than by reasoning about
the SQL alone.

The third surprise was that this exact NULL-vs-blank trap could still bite
after the fact, even once I knew about it. Trimming the fallback key from 8
to 6 columns (dropping `DEEDBOOK`/`DEEDPAGE`, since they're always blank in
that subset) seemed like a safe simplification. But the code that normalizes
blank values to `''` was keyed off key-column membership, not column
identity, so once those two columns left the key, they stopped getting
normalized and started landing as true SQL `NULL`. Both partial indexes test
`DEEDBOOK = ''`/`!= ''`, and `NULL = ''` is neither true nor false in SQL,
so those ~600 rows silently lost duplicate protection entirely. Running the
pipeline twice to verify re-runnability caught it immediately: the row count
grew by exactly 600 on the second run. Fixed by normalizing `DEEDBOOK`/
`DEEDPAGE` unconditionally, independent of which key is active. The lesson
repeats the first one: a "safe-looking" change still needs to be run against
real data twice, not reasoned through once.

## AI disclosure

I built this with Claude Code (Anthropic's CLI agent) as my primary tool
through the whole exercise. I directed the architecture through discussion,
including the choice of SQLite, the upsert key design (and specifically
rejecting `PARID` alone and rejecting including `PRICE`/`SALECODE` in the
key), the flag-vs-drop rule for anomalies, and the check thresholds, and
Claude implemented and tested each piece against the live dataset. We found
and fixed three real bugs together by testing against real data rather than
trusting the SQL in the abstract: a NULL-handling bug that silently excluded
2,711 valid rows from the clean table, a partial-index collision bug where
normalizing blank deed info for one upsert key unexpectedly collided with
the other key's index, and a NULL-normalization regression introduced while
simplifying the fallback key, caught by re-running the pipeline twice and
watching the row count grow by exactly 600. I reviewed the design decisions,
the sale-code classification, and the quality-check logic above, and can
explain and defend each one without notes.
