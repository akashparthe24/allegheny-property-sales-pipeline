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

Stack: SQLite. No setup, no server process, real `UNIQUE` indexes and
`ON CONFLICT` upserts for re-runnability, and it ships with Python. At real
concurrency or 100x scale, Postgres or DuckDB would be the better choice.

## Design decisions

**1. Upsert on `(PARID, DEEDBOOK, DEEDPAGE)`, not a full reload every run.**
The point: the pipeline needs to tell "this is a new transaction" apart from
"this is the same transaction, maybe corrected," which a `DROP`+reload every
run can't do at all, it just throws every row away and reloads, with no
concept of anything having changed since last time. That's a real problem
once a pipeline runs daily rather than once.

So the real question was what, in the raw data itself, actually identifies
one specific transaction. I ruled out the obvious candidates by checking them
against the live data, not by guessing:
- `_id` (CKAN's internal row number): useless. It regenerates on a full
  source reload, so the same real transaction gets a different `_id` next
  time, two runs of identical data would look 100% "different."
- `PARID` alone: fails immediately. Grouping by `PARID` shows many parcels
  have 2+ rows (one has 24 across 14 years). A parcel can legitimately sell
  many times, so this would collapse real transaction history down to one
  row per property, it's a foreign key to the property, not a key for a sale.
- `SALEDATE`, `PRICE`, `SALECODE` individually: not unique, each repeats
  constantly across unrelated sales. `PRICE`/`SALECODE` specifically can't be
  *part of* a composite key either, since those are exactly the fields the
  county corrects after the fact; if they were in the key, a correction would
  look like "a new row appeared" instead of "an existing row changed."

What's actually left in the data: `DEEDBOOK`+`DEEDPAGE`, the county's own
recording reference, literally which document page this specific deed was
filed on. `(PARID, DEEDBOOK, DEEDPAGE)` is the closest thing the raw data has
to a natural, source-defined transaction identity, not something invented for
this project. The one gap: ~600 rows (0.1%) have both fields blank, so those
fall back to a wider 6-column match on the remaining descriptive fields
(`PARID, RECORDDATE, SALEDATE, INSTRTYP, PRICE, SALECODE`) instead.
**Drawback:** two keys means two upsert statements and two partial indexes
instead of one simple `DROP`+reload, and this complexity is exactly what cost
real debugging time, not hypothetically. Three separate bugs surfaced while
building and later refining this (see "What surprised me" for all three): a
NULL-vs-blank duplication bug, a partial-index collision between the two
keys, and a NULL-normalization regression introduced while trimming the
fallback key down to 6 columns. A full reload would have been correct on day
one with zero bugs possible; this needed real testing, three times over, to
actually trust.

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
calculation specifically. Only rows that are genuinely unusable, meaning a
blank `PARID`, `SALEDATE`, `SALECODE`, `INSTRTYP`, or `MUNICODE`, an
unparseable sale date, or a negative price, are dropped, and that's a
deliberate, documented rule, not a silent side effect of a quality check.
**Drawback:** the clean table is not actually "clean" in the sense of being
safe to aggregate blindly. Anyone querying it has to know to filter on the
flag columns for a market-price calculation, or they'll silently include $1
gift sales in their numbers. A pre-filtered table would be harder to misuse,
at the cost of hiding information.

**3. Validation happens entirely after loading, not split pre/post.**
The point of validating anything before the real load is to avoid wasting
time and I/O loading data that's just going to get rejected anyway, and to
stop obviously-bad data before it ever touches a real table. That's a
reasonable goal, so I actually built two checks around it: a schema check
(did the API still have every required column) and a row-count guard (did
the total move more than a normal day's activity would explain), both run
against just the first API page.

I rejected keeping either, after actually working with them, not in theory.
The row-count guard needed a standing override (`ALLOW_ROW_COUNT_DROP=1`)
just to survive this source's routine volatility (one pull during this build
came back 21% smaller than one from three days earlier, confirmed real, not
a broken download), and a check a human has to override regularly isn't
protecting anything, it's a logged shrug. The schema check turned out to be
redundant for a different reason: the load table is built from whatever
columns the API actually returns, so a genuinely missing column isn't
silently ignored, it surfaces as a hard SQL error the moment any check or
transform references that column by name, the pre-load version of that check
wasn't catching anything a post-load failure wouldn't catch anyway. So all
validation now runs post-load, against the real data, where the ten checks
(duplicate keys, not-null, price, dates, timestamps) catch correctness
problems directly instead of guessing at them from page one.
**Drawback:** a bad pull now always finishes writing to `property_sales_load`
before anything catches it, so a truly broken run costs more wasted I/O than
it would have with a pre-load gate. And a missing column fails as a generic
SQL error rather than a clear, named "these columns disappeared" message,
which costs a reader a few extra seconds working out what actually broke.

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
