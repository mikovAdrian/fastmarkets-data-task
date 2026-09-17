# Fastmarkets Data Engineer Take-Home

A two-phase pipeline. A Python script pulls a CSV order extract over HTTP and
lands it in a Snowflake raw layer with the nested `order_items` array stored as
`VARIANT`. dbt then models it into a Kimball star schema and exposes weekly
product sales with a revenue-based top-seller flag.

Verified end to end against a Snowflake trial: 1,000 orders, 1,674 line items,
1,000 customers, 424 aggregate rows across 144 weeks, 102 dbt tests and 24
loader unit tests passing.

## Stack

- Python 3.14
- snowflake-connector-python
- dbt-core 1.12 with dbt-snowflake
- Snowflake

### Why local Python rather than Snowpark or Notebooks

The task allows either. I chose to run Python locally for three reasons.

**It is easier to test.** The loader has 24 unit tests that run against a fake
HTTP response and a fake Snowflake connection, with no credentials and no
warehouse. A Snowpark notebook cannot be tested that way: verifying it means
executing it against a live session.

**It version-controls properly.** A notebook is JSON on disk, so a diff is
unreadable and a code review is guesswork. A `.py` file diffs line by line,
which is what made the adversarial review pass described under *Use of AI*
possible at all.

**It fits the orchestration story.** The script has a plain `main()` entry
point and takes its configuration from the environment, so it drops into a
Dagster op or an Airflow task unchanged. A notebook would tie scheduling to
Snowflake.

What I gave up: Snowpark would push the transformation into the warehouse and
avoid pulling data through the client process. That matters at the volumes
described under *Loading at scale* - and there the right answer is `COPY INTO`
from a stage, not Snowpark.

## Repository layout

    load/          Python extract-and-load script and its pinned dependencies
    load/tests/    unit tests for the loader (no credentials, no network)
    dbt_project/   dbt transformation project (staging -> marts)
    data/          local working directory for the extract (gitignored)
    .github/       CI running the loader's unit tests

## Setup

### 1. Python environment

    python3 -m venv venv
    source venv/bin/activate
    pip install -r load/requirements.txt
    pip install dbt-snowflake

`dbt-snowflake` is kept out of `load/requirements.txt` deliberately: the loader
and the dbt project are two separately deployable components. In production the
loader would ship in an orchestrator task image with no reason to carry
dbt-core and its dependency tree.

### 2. Credentials

    cp .env.example .env

`.env` is gitignored and never committed. The loader reads plain environment
variables and calls `load_dotenv()` only as a local convenience, so in
production the same contract is satisfied by an orchestrator's secret backend
with no `.env` file present.

Required: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
`SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SOURCE_CSV_URL`. Optional with
defaults: `SNOWFLAKE_DATABASE` (`FASTMARKETS`), `SNOWFLAKE_RAW_SCHEMA` (`RAW`),
`SNOWFLAKE_DBT_SCHEMA` (`DBT_DEV`).

### 3. Snowflake prerequisites

The loader creates its schema but not the database or the warehouse: creating
databases is an administrative act, not a pipeline concern.

    CREATE DATABASE IF NOT EXISTS FASTMARKETS;

    CREATE WAREHOUSE IF NOT EXISTS FASTMARKETS_WH
        WAREHOUSE_SIZE = 'XSMALL'
        AUTO_SUSPEND = 60
        AUTO_RESUME = TRUE
        INITIALLY_SUSPENDED = TRUE;

`AUTO_SUSPEND = 60` rather than the 600-second default: every dbt run would
otherwise leave ten minutes of paid idle time behind it.

### 4. dbt profile and packages

    mkdir -p ~/.dbt
    cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml
    cd dbt_project && dbt deps

The example profile holds no credentials at all - every value is an `env_var()`
lookup - so it can be copied verbatim. The loader and dbt are driven by the
same environment variables, so there is one environment contract, not two.

## Running the pipeline

### Phase 1 - extract and load

    python load/load_to_snowflake.py

Reads `SOURCE_CSV_URL`, overridable with `--source-url`. Exit codes: `0`
success, `1` runtime failure, `2` usage problem such as a missing source URL.
The distinction matters to a scheduler deciding whether a retry is worthwhile.

### Phase 2 - transform and test

    cd dbt_project
    set -a && . ../.env && set +a
    dbt build

**dbt does not read `.env`.** `python-dotenv` only populates the environment
inside the Python process; `env_var()` in `profiles.yml` reads the real
environment. Sourcing `.env` first bridges the two. Quote any value containing
shell metacharacters, or `source` will interpret them.

`dbt_project.yml` pins `require-dbt-version: [">=1.12.0", "<1.13.0"]` for the
same reason the Python dependencies are pinned: a reviewer running this later
should get the behaviour it was built on.

### Loader unit tests

    pip install -r load/requirements-dev.txt
    pytest load/tests -q

24 tests against a fake HTTP response and a fake Snowflake connection, so they
need no credentials and no network. They cover the parsing rules, the audit
columns, the statement ordering inside `load()` and the CLI exit codes. CI runs
them on every push; the dbt models are not built in CI because that needs live
warehouse credentials.

## Data model

### Raw layer

`RAW.ORDERS_RAW` mirrors the source one-big-table: one row per source CSV row.
Every business column is `STRING` except `order_items`, which is `VARIANT` so
the nested array stays queryable in place. Typing and validation are staging
concerns - the raw layer records what arrived.

Two audit columns are added on load: `_loaded_at`, the UTC load timestamp, and
`_source_row_hash`, a SHA-256 of the business columns.

To be precise about the hash: **nothing consumes it yet.** It is carried through
to `fct_order` but no model or test reads it, and under full-snapshot reload it
could not do the job it is named for - comparing runs requires keeping the
previous state, which reloading discards. It exists as the mechanism the
incremental design under *Going further* needs, and it is more honest to call it
preparation than an active control.

### Staging - views

| Model | Grain | Responsibility |
| --- | --- | --- |
| `stg_orders` | one order | typing and trimming; `order_items` passed through |
| `stg_order_items` | one line | `LATERAL FLATTEN` of the array |
| `stg_customers` | one customer | contact cleanup, validity flags, dedup |

### Marts

| Model | Grain | Materialization | Rows |
| --- | --- | --- | --- |
| `dim_customer` | one customer | table | 1,000 |
| `fct_order` | one order | table | 1,000 |
| `fct_order_item` | one order line | table | 1,674 |
| `agg_weekly_product_sales` | one week per product | view | 424 |

## Modeling decisions

### Kimball star schema

The data is a static snapshot with no requirement for a historical audit trail,
and the goal - weekly aggregation by product - is a textbook BI question that a
star schema is directly optimized for.

**Data Vault** was considered and rejected. It would give full auditability
through hub/link/satellite separation, but that machinery earns its cost with
multiple source systems and a hard history requirement. For a one-off extract
of a thousand rows it is overhead without payoff.

**3NF** was also considered - correct, but it maps less directly onto the
aggregation requirement and would not make the analytical intent clearer.

### Two things deliberately left out

**No `dim_product`.** The star has one dimension; `product_id`,
`product_name` and `unit_price` sit on `fct_order_item`. For `unit_price` that
is correct - the price on a line is a historical value and belongs to the fact,
not to a dimension that would overwrite it. For `product_name` it is a
compromise: it is a pure dimension attribute repeated across 1,674 rows, and
with three products and a strict 1:1 mapping to `product_id` a dimension table
would add a join without adding information. With a real product catalogue -
categories, suppliers, hierarchies - it would earn its place.

**No date spine.** The weekly aggregate contains only weeks that had sales. In
this extract that costs nothing: 144 weeks span the range and none are missing.
But a week with no orders simply does not appear, so a report reading the
aggregate directly would show a gap rather than a zero. `dbt_utils.date_spine`
joined to a `dim_date` is the standard fix and is the first thing I would add
for recurring reporting.

### Two dbt layers, not raw straight to marts

Cleanup logic lives in staging and only in staging. Inlined in the marts it
would be duplicated across every model touching customers and would drift apart
over time.

This is why the staging models are thin. `quantity * unit_price` is a derived
measure, so `line_total` is defined in `fct_order_item`; surrogate keys are a
mart concept for the same reason.

`fct_order` reads `fct_order_item` rather than `stg_order_items` so the
multiplication is written in exactly one place. A fact reading another fact is a
dependency worth accepting: the alternative repeats the expression, and the two
copies would drift the moment a discount or tax term is added to one of them.

One duplication is left in place knowingly. `stg_customers` reads the source
directly rather than `stg_orders`, because `stg_orders` does not carry the
contact columns, so `try_to_date(order_date)` is written in both models. Routing
customer attributes through a model whose grain is orders would be worse than
repeating one cast; the alternative is a third staging model that exists only to
type the shared columns, which is more machinery than the problem deserves.

### Load: full-snapshot reload

The source is a complete extract, not a delta feed, so each run replaces the
table contents. That makes the load idempotent by construction - verified by
running it twice and confirming 1,000 rows rather than 2,000.

An incremental version is sketched under *Going further*; `_source_row_hash`
exists to make its change detection cheap.

### Two-step load into VARIANT

`write_pandas` cannot write a `VARIANT` column from a Python string, so the
frame lands in a transient staging table with `order_items_raw` as `STRING`,
then one `INSERT OVERWRITE ... SELECT PARSE_JSON(order_items_raw)` casts it
across. `write_pandas` itself performs a `PUT` and `COPY INTO` against a
temporary internal stage, so this is a convenience wrapper, not a different
mechanism.

`INSERT OVERWRITE` rather than `TRUNCATE` then `INSERT`: one atomic statement,
no window where the raw table is empty. `TRUNCATE` is DDL in Snowflake and
commits on its own, so the two-statement version could not have been
transactional anyway.

The staging table is dropped **on success only**. On failure it is left in
place and logged: it holds the un-parsed `order_items` strings and is the only
thing that can identify which row `PARSE_JSON` rejected. That is also why it is
`TRANSIENT` and not `TEMPORARY` - a temporary table vanishes with the session,
including on the failure that needs inspecting.

### Dirty input is flagged, not cleaned, in the loader

The extract carries known defects: 975 phone values wrapped in literal
double-quote characters from a source double-escaping bug, 486 emails in mixed
case, and placeholders (`N/A`, `invalid-email`, `123456`, empty strings) mixed
in with valid values.

`read_csv` runs with `dtype=str` and `keep_default_na=False`, so no type
inference occurs and `N/A` stays the literal string `N/A` instead of collapsing
into `NaN` alongside genuinely empty fields. Those are two different upstream
defects - a broken integration writing a placeholder, versus an absent value -
and the raw layer should still distinguish them.

`stg_customers` strips the quotes, lowercases emails and flags validity.
Invalid values are flagged but never deleted: the raw value stays next to the
flag so consumers decide whether to trust it. Of 1,000 customers, 900 have a
valid phone and 913 a valid email; `has_valid_contact` combines them and
reveals the number neither flag shows alone - **15 customers cannot be reached
at all**.

### Customer deduplication is SCD Type 1

`stg_customers` collapses the one-big-table with
`QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC
NULLS LAST, _loaded_at DESC) = 1`, keeping each customer's most recent order on
the assumption that contact details are more current there. `NULLS LAST` is
deliberate: Snowflake sorts nulls first on a `DESC` order, so without it a
customer whose date failed to parse would win.

**In this extract the dedup is a no-op** - 1,000 orders and 1,000 distinct
customers, a 1:1 relationship. The logic is defensive, kept because a real feed
will repeat customers, and the uniqueness test on `customer_id` is what would
catch the model breaking if that changed. SCD Type 2 would be the answer if
tracking *when* details changed were a requirement; it is not one here.

### Top seller by revenue, not quantity

Revenue answers "which product made us the most money", the standard commercial
question. Quantity answers "which product moved the most units", which matters
for inventory. `total_quantity` is kept as a column so the quantity view stays
queryable without a schema change.

**The choice is not academic here.** `B1 Gadget` sells at 150 while `A1 Widget`
and `C1 Doohickey` sell at 50, and the two metrics disagree on the top seller
in **80 of the 144 weeks**. Across the extract `B1` leads on revenue with
149,100 against `A1`'s 50,800, while `A1` leads on units with 1,016 against
`B1`'s 994.

`RANK()`, not `ROW_NUMBER()`: with `ROW_NUMBER()` a genuine tie is broken
arbitrarily and one of two equal top sellers is silently dropped, so the output
looks correct while being wrong. There are **no revenue ties in this extract**,
so both functions would produce identical output - the choice is defensive.
Ties do exist on the other metric: in the week of 2023-03-20 `A1` and `C1` tie
on quantity.

The aggregate is a **view**, per the task's instruction and because it is a
cheap aggregation over an already-materialized fact.

It carries both `line_item_count` and `order_count`. They differ only when a
product appears on more than one line of the same order, which never happens
here - **the two columns are identical in all 424 rows.** They are kept separate
because the distinction is real in a feed that allows it, but on this data one
of them carries no information.

### Known environment dependency: `WEEK_START`

`DATE_TRUNC('week', ...)` depends on Snowflake's `WEEK_START` session
parameter. This account uses the default of `0`, which is ISO behaviour and
aligns weeks to Monday - confirmed by the earliest `week_start` being
2022-12-26, the Monday before the first order. **An account with a different
`WEEK_START` would produce different week boundaries from identical code.**
Pinning it would mean an `on-run-start` hook; it is documented rather than
pinned because the reviewer account uses the default.

## Data quality and testing

102 dbt tests: 10 on the source, 30 on staging, 59 on the marts, plus three
singular tests. All pass, alongside 24 unit tests for the loader.

Documentation is complete rather than partial: all 7 models, all 56 model
columns, all 10 source columns and all 3 singular tests carry descriptions, so
`dbt docs generate` produces a full catalogue rather than a skeleton:

    cd dbt_project
    dbt docs generate
    dbt docs serve

The generated site is not committed - `target/` is gitignored, because
generated artefacts in version control go stale the moment a model changes.
The descriptions that produce it are in `_sources.yml`, `_staging.yml` and
`_marts.yml`.

Tests cover failure modes that do not occur in this sample, not just the ones
that do. Production feeds change, and a suite that only encodes today's data
will not notice tomorrow's regression.

### Two kinds of test, and it matters which is which

A test count on its own is close to meaningless, so it is worth being explicit
about what these 102 tests actually protect.

**Data guards** can fail when the incoming data changes. `not_null` on
source-derived columns, `unique` on `order_id` at the source, `quantity > 0`,
`unit_price >= 0`, `accepted_values` on `product_id`, `has_total_mismatch`, and
the singular tests asserting every week has a top seller and every order has
line items. These are the tests that would tell me something new tomorrow.

**Structural guards** cannot fail as the models are written today, because the
model construction already guarantees them. All six `relationships` tests fall
into this group: `stg_order_items` is built *from* `stg_orders`, so every
`order_id` in the child necessarily exists in the parent; `dim_customer` is a
deduplication of the same source column `fct_order` reads, so an orphan
customer is not reachable. The same applies to
`unique_combination_of_columns (week_start, product_id)` on a model whose
`GROUP BY` is those two columns, to `revenue_rank >= 1` when the column is a
`RANK()`, and to `accepted_values [true, false]` on native `BOOLEAN` columns.

That is not an argument for deleting them. They are a contract on the shape of
the model rather than a check on its contents, and they fail exactly when
someone changes an `INNER JOIN` to a `LEFT JOIN`, drops a `GROUP BY`, or
re-implements a flag as `'Y'`/`'N'` - refactors that every column-level test
would otherwise wave through. But claiming "referential integrity is verified"
would overstate it: referential integrity here is *constructed*, and the tests
document that construction.

Counted precisely: **24 of the 102 are structural, and 78 can fail on new
data.** The 24 are the six `relationships` tests, three
`unique_combination_of_columns`, six `accepted_values [true, false]` on native
booleans, three `unique` tests on keys a `QUALIFY` or a surrogate hash already
guarantees, five `accepted_range` bounds on values that are sums or ranks of
non-negative inputs, and `assert_order_line_counts_reconcile`.

| Category | Coverage |
| --- | --- |
| Uniqueness | `customer_id`, `order_id`, `order_item_id`, and the composites `(order_id, line_number)` and `(week_start, product_id)` |
| Not null | every key, flag and measure column |
| Referential integrity | `fct_order_item` to `fct_order` to `dim_customer`, plus the direct line-to-customer edge |
| Value ranges | `quantity > 0`, `unit_price >= 0`, `order_total >= 0`, `revenue_rank >= 1` |
| Accepted values | `product_id` in `A1`/`B1`/`C1`, and the boolean flags |

`order_id` uniqueness is tested at the **source**, not only downstream. If the
feed starts sending duplicates that should surface at the boundary, not after
they have propagated through staging into three marts.

`accepted_values` on `product_id` is `severity: warn`. A fourth product is
normal business change, not a data defect, and should not stop a pipeline.

### Header total versus line totals

`fct_order.has_total_mismatch` is exposed as a column, so an analyst sees it
without reading the dbt project, **and** tested with `severity: warn`. A real
feed can legitimately carry discounts, tax or shipping in the header that the
lines do not account for; that deserves investigation, not a failed build.

The `COALESCE` in its definition is load-bearing. An order with no line items
would give a null `computed_total`, and `null != order_total` evaluates to null
rather than true in SQL's three-valued logic - so the mismatch would be
invisible in exactly the case that most deserves attention.

**In this extract there are zero mismatches**: all 1,000 orders reconcile, and
`order_total`, `line_total` and `total_revenue` each sum to 233,100.00 across
the three layers. The column detects nothing here and is kept for the feed this
would become.

### The singular tests

**`assert_each_week_has_a_top_seller.sql`** asserts every week with sales has
at least one product flagged.

No column-level test can catch this. `accepted_values` proves `is_top_seller`
holds only true or false; `not_null` proves it is populated. Neither notices a
refactor that leaves the flag false for an entire week - `RANK` swapped for
`DENSE_RANK` with a wrong offset, or a partition clause that loses a week.

It tests `COUNT_IF(is_top_seller) = 0`, not `!= 1`. On a genuine tie two
products rank 1 and the week correctly has two top sellers; a test for exactly
one would fail precisely when the code works as designed.

One limitation worth naming: the week of 2022-12-26 contains a single product,
which is therefore top seller by default. The flag carries no information for a
week with one product, and this is not addressed.

**`assert_every_order_has_line_items.sql`** catches an order that arrives with
an empty `order_items` array. Such an order survives the load and stays in
`fct_order` - dropping it would be worse - but contributes nothing to
`fct_order_item` or the weekly aggregate, so whatever its header total claimed
disappears from revenue reporting. `has_total_mismatch` would flag it at warn
severity, which is right for a header-versus-lines disagreement in general but
too quiet for an order with no lines at all, so this is an error.

**`assert_order_line_counts_reconcile.sql`** asserts that `fct_order`'s
degenerate measures agree with the line grain they summarise:
`SUM(line_item_count)` against the row count of `fct_order_item`, and the same
for quantity and revenue. This is a structural guard in the sense described
above - `fct_order` derives those measures from `fct_order_item`, so it cannot
fail today. It fails if either fact's grain changes.

### Lineage

`_loaded_at`, written by the loader, is carried through to `fct_order`, so any
row in the marts traces back to the load run that produced it. This was
verified by checking that `MAX(_loaded_at)` is identical in `RAW.ORDERS_RAW`
and `fct_order` after a full run.

## Use of AI

I used Claude (Claude Code) as a design partner and reviewer, and Snowflake's
Snowsight assistant for account and access questions.

The pattern was deliberate: I made the design decisions and wrote every file
myself, then had Claude argue the alternatives, review what I had written, and
verify the output against numbers I had stated in advance. I wanted to be able
to defend every line, and that means deciding and typing it.

### The prompts that did the work

**Holding the specification against a sound-sounding argument.**

> "I don't want to deviate from the brief, regardless of whether the things
> they're asking for are valid for this dataset or not."

I had specified `accepted_values` tests on the boolean flag columns. The
suggestion was to remove them as empty ceremony, since a native `BOOLEAN`
cannot hold anything else - technically true. I put them back. A test that
guards a column's contract still earns its place, and dropping a requirement
because it is inconvenient for one sample is the wrong instinct.

**Refusing code without a justification.**

> "Why are we dropping it?"

About the transient staging table. The answer exposed a real flaw: the `DROP`
sat in a `finally` block, so it ran on failure too - destroying the only copy
of the un-parsed `order_items` strings, which is the one thing that can
identify which row `PARSE_JSON` rejected. It also undercut the choice of
`TRANSIENT` over `TEMPORARY`, which exists precisely so the table survives a
failed session. Three words changed the design.

**Turning the tool against the finished work.**

> "I want a check at every level - business decisions, implementation
> decisions, implementations, tests. The deepest review you are capable of, as
> if you had nothing to do with this project and were reading it for the first
> time."

The most productive prompt I wrote. It surfaced four gaps I had not seen: no
unit tests for the loader despite the README claiming the logic was verified,
`quantity * unit_price` written in two models that could drift apart, a
cross-layer reconciliation the README described but no test performed, and an
unpinned dbt version in an otherwise strictly pinned project. It also caught a
false statement in my own README: I had written that roughly 20 of the tests
were structural, and counting them properly gave 24.

**Demanding proof in the real system, not locally.**

> "We need to be certain the data is there at both phases - phase one with
> Python and phase two with dbt - and that it actually did that in Snowflake."

I did not want "it passes locally". The run that followed reloaded from the
source URL and rebuilt every model, then proved the chain rather than asserting
it: `_loaded_at` written by the Python loader appears unchanged in `fct_order`,
every object's `last_altered` timestamp falls inside the run window, and
`order_total`, `line_total` and `total_revenue` each sum to 233,100.00 across
the three layers.

**Pressing on a confident claim until it broke.**

> "Why?"

About the choice of `\x1f` as the delimiter when hashing a row's business
columns. The comment in the code implied it prevents collisions. Pressing on it
established that it does not: a delimiter that can appear inside a value makes
two different rows hash identically - demonstrated with a comma, where
`["C1","Ann","Smith,London"]` and `["C1","Ann,Smith","London"]` produce the
same digest - and `\x1f` is merely very unlikely to occur, not impossible. The
collision-proof answers are length-prefixing each field or hashing a canonical
serialization. The code kept `\x1f` as a proportionate choice; this README
states the limit rather than overclaiming it.

### Judgement calls

**Suggestions I rejected.** Implementing both `write_pandas` and a manual
`PUT` + `COPY INTO` path - two redundant paths for a thousand-row file signals
indecision, not thoroughness. Removing `accepted_values` from the boolean flags
as "empty ceremony" - a test guarding a column's contract still earns its
place. Building this on Dagster - the loader is already orchestration-ready
through a plain `main()` and environment config, and a half-wired orchestrator
is worse than a documented sketch.

**Suggestions I took.** `INSERT OVERWRITE` instead of `TRUNCATE` then `INSERT`,
because `TRUNCATE` is DDL in Snowflake and commits on its own. Dropping the
staging table only on success, because a `finally` block destroys the evidence
in the one case that needs it.

**Where I changed my mind.** I argued for an incremental load with history,
since orders get updated in the real world. What settled it was factual: there
is one static file, so an incremental load has nothing to be incremental
against. I kept the full reload and documented the incremental design.

**What the verification loop caught.** Every mocked test passed, but the live
run warned about `use_logical_type` - and it was right. `write_pandas` wrote the
timezone-aware `_loaded_at` as naive wall-clock and Snowflake reinterpreted it
in the session timezone, seven hours out. Mocks verify logic and call ordering,
not how a driver serializes types.

It also contradicted my own plan. I had assumed customer attributes repeat
across orders, which is why the dedup exists; the data has 1,000 orders and
1,000 distinct customers, so it removes nothing. Same for `has_total_mismatch` -
all 1,000 orders reconcile. Both are documented as defensive rather than
active, because claiming a mechanism does something it does not is worse than
not having it.

Not everything produced was correct. A password-policy property name and a
column alias that would have silently created a duplicate column were both
wrong and caught before they mattered.

### What I did to make the tooling more effective

**I wrote the design down before writing any code.** I put the architecture -
the modelling approach, the load mechanism, the layering, the definition of
"top seller" - into a single brief and had it reviewed and criticised before a
line existed. That document then travelled with every session, so decisions
already made were not silently relitigated later. Most of the wasted motion I
have seen with AI tooling comes from the model re-deciding on turn forty
something that was settled on turn three; a written brief is the cheapest fix.

**I asked it to attack the plan rather than confirm it.** After that review I
pushed back on specific points to see which arguments held. That is how Data
Vault came to be rejected on cost rather than on taste, and how the
full-reload decision survived a challenge I had raised myself.

**I fixed the division of labour and kept it.** I made the decisions and wrote
every file; the tooling argued alternatives, reviewed what I had written, and
verified it against numbers I had stated in advance. I asked for code in the
chat rather than having files generated, because I have to defend this in a
room.

**I used it as an adversary at the end, not only as a helper.** The
deepest-review prompt above is the clearest example, and repeating the same
audit after the fixes were applied is what caught the incorrect test count.

**Snowsight assistant.** Used for the access side rather than modelling: the
role and grant structure for a least-privilege reviewer account, and the
behaviour of Snowflake's password policy when creating versus altering a user.
I verified its output by running `SHOW GRANTS` and `SHOW FUTURE GRANTS` and
reading the result rather than assuming the statements did what they claimed.

**On speed.** Most of the gain was not typing - it was closing the verification
loop in seconds: state the expected number, run it, compare. Every row count in
this README was predicted before it was measured.

## Going further

### Loading at scale

`write_pandas` pulls the whole extract through the client process, which is
fine for a small file and wrong for a continuous feed. The next step is to stop
moving data through the orchestrator's memory and let Snowflake read directly
from cloud storage:

    CREATE OR REPLACE STAGE raw.orders_stage
      URL = 's3://bucket/orders/'
      STORAGE_INTEGRATION = s3_int
      FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

    COPY INTO raw.orders_raw (customer_id, ..., order_items)
      FROM (SELECT $1, ..., PARSE_JSON($8) FROM @raw.orders_stage)
      PATTERN = '.*orders_.*[.]csv'
      ON_ERROR = CONTINUE;

The gains: the orchestrator task only issues SQL, `PATTERN` picks up whole
batches in one statement, `ON_ERROR = CONTINUE` keeps one malformed file from
failing a batch, and Snowflake's load metadata makes `COPY` skip files it has
already ingested.

### Orchestration

The loader is a plain script with a `main()` entry point and environment-based
configuration, so it drops into a Dagster op or an Airflow task unchanged, with
`dbt build` downstream. With `dagster-dbt` the dbt DAG is derived from
`manifest.json`, so lineage in the UI matches lineage in the project with
nothing duplicated:

    @asset
    def orders_raw(context) -> MaterializeResult:
        rows = load(get_connection(), df_to_variant_ready(fetch_csv(SOURCE_CSV_URL)))
        return MaterializeResult(metadata={"rows": rows})

    @dbt_assets(manifest=DBT_MANIFEST)
    def fastmarkets_dbt(context, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

Snowflake Tasks would keep everything inside the warehouse and avoid a separate
scheduler, at the cost of weaker observability and no shared lineage.

### Incremental loading

Full reload is right for a full extract. For a delta feed the options are a
`MERGE` on `order_id` for upsert semantics, or an append-only raw layer with a
`_loaded_at` high-watermark that keeps every version ever received. The second
turns `_source_row_hash` into the mechanism rather than a monitoring hook:

    INSERT INTO orders_raw (...)
    SELECT s.* FROM _orders_raw_staging s
    WHERE NOT EXISTS (
        SELECT 1 FROM orders_raw r
        WHERE r.order_id = s.order_id AND r._source_row_hash = s._source_row_hash
    )

That is idempotent, keeps history, and lands only genuinely changed rows;
`stg_orders` would then select the latest version per `order_id` with
`QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1`.

### Performance and cost

Measured in the trial account rather than estimated.

**The whole project cost 0.86 credits** - three days of development with dozens
of rebuilds, against the 400 a trial provides. Storage is 0.25 MB across all
eight objects, which rounds to nothing.

**A single end-to-end run** is about 20 seconds of query time on an X-Small
warehouse. X-Small bills at one credit per hour, charged per second with a
60-second minimum after each resume, so one run costs roughly **0.017
credits** - and the 60-second floor, not the work, is what dominates.

| Schedule | Credits per year |
| --- | --- |
| Daily | ~6 |
| Hourly | ~146 |

That 60-second floor is why the warehouse is configured with
`AUTO_SUSPEND = 60` instead of the 600-second default: ten minutes of paid idle
after every run would cost an order of magnitude more for identical work. It is
also why an hourly schedule costs 24 times a daily one while doing the same
trivial amount of computing - you are paying for resumes, not for compute.

At this size the interesting cost question is not the warehouse at all.
`write_pandas` moves the whole extract through the client process, so the real
cost sits in the orchestrator's memory and runtime rather than in Snowflake.
The stage-based `COPY INTO` approach above moves it back to where it can be
measured and scaled.

### Observability

The load logs row counts at each step, which is the minimum needed to spot a
silently truncated extract. Beyond that: persist per-run row counts and
`_source_row_hash` distributions to detect drift, alert on dbt test warnings
rather than only errors, and add `dbt source freshness` once the feed recurs.

## Reviewer access

A dedicated read-only user is provisioned in the Snowflake trial. The account
identifier, user name and a temporary password are sent separately; the
password must be changed on first sign-in.

The role holds `USAGE` on the warehouse, database and three schemas plus
`SELECT` on their tables and views - 13 grants, none of which permit writing.
It cannot insert, update, delete, create or drop anything.

It also holds **future grants** on all three schemas. `dbt build` issues
`CREATE OR REPLACE`, so each run produces new objects that do not inherit the
previous grants; without future grants the reviewer's access would disappear at
the first rebuild, silently. This was verified by rebuilding and confirming
access to the new objects.
