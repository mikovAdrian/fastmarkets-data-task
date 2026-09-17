# Fastmarkets Data Engineer Take-Home

A two-phase pipeline. A Python script pulls a CSV order extract over HTTP and
lands it in a Snowflake raw layer with the nested `order_items` array stored as
`VARIANT`. dbt then models it into a Kimball star schema and exposes weekly
product sales with a revenue-based top-seller flag.

Verified end to end against a Snowflake trial: 1,000 orders, 1,674 line items,
1,000 customers, 424 aggregate rows across 144 weeks, 93 tests passing.

## Stack

- Python 3.14
- snowflake-connector-python
- dbt-core 1.12 with dbt-snowflake
- Snowflake

## Repository layout

    load/          Python extract-and-load script and its pinned dependencies
    dbt_project/   dbt transformation project (staging -> marts)
    data/          local working directory for the extract (gitignored)

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

## Data model

### Raw layer

`RAW.ORDERS_RAW` mirrors the source one-big-table: one row per source CSV row.
Every business column is `STRING` except `order_items`, which is `VARIANT` so
the nested array stays queryable in place. Typing and validation are staging
concerns - the raw layer records what arrived.

Two audit columns are added on load: `_loaded_at`, the UTC load timestamp, and
`_source_row_hash`, a SHA-256 of the business columns so monitoring can detect
the same `order_id` arriving with different content between runs.

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

### Two dbt layers, not raw straight to marts

Cleanup logic lives in staging and only in staging. Inlined in the marts it
would be duplicated across every model touching customers and would drift apart
over time.

This is why the staging models are thin. `quantity * unit_price` is a derived
measure, so `line_total` is defined in `fct_order_item`; surrogate keys are a
mart concept for the same reason.

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

### Known environment dependency: `WEEK_START`

`DATE_TRUNC('week', ...)` depends on Snowflake's `WEEK_START` session
parameter. This account uses the default of `0`, which is ISO behaviour and
aligns weeks to Monday - confirmed by the earliest `week_start` being
2022-12-26, the Monday before the first order. **An account with a different
`WEEK_START` would produce different week boundaries from identical code.**
Pinning it would mean an `on-run-start` hook; it is documented rather than
pinned because the reviewer account uses the default.

## Data quality and testing

93 tests: 7 on the source, 31 on staging, 54 on the marts, plus one singular
test. All pass.

Tests cover failure modes that do not occur in this sample, not just the ones
that do. Production feeds change, and a suite that only encodes today's data
will not notice tomorrow's regression.

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

### The singular test

`tests/assert_each_week_has_a_top_seller.sql` asserts every week with sales has
at least one product flagged.

No column-level test can catch this. `accepted_values` proves `is_top_seller`
holds only true or false; `not_null` proves it is populated. Neither notices a
refactor that leaves the flag false for an entire week - `RANK` swapped for
`DENSE_RANK` with a wrong offset, or a partition clause that loses a week.

It tests `COUNT_IF(is_top_seller) = 0`, not `!= 1`. On a genuine tie two
products rank 1 and the week correctly has two top sellers; a test for exactly
one would fail precisely when the code works as designed.

### Cross-layer reconciliation

`SUM(line_item_count)` in `fct_order` must equal the row count of
`fct_order_item` - 1,674 - which makes drift between the two facts detectable
with one query. `_loaded_at`, written by the loader, is carried through to
`fct_order`, so the marts trace back to the load run that produced them.

## Use of AI

I used Claude (Claude Code) as a design partner and reviewer, and Snowflake's
Snowsight assistant for account and access questions.

The pattern was deliberate: I made the design decisions and wrote every file
myself, then had Claude argue the alternatives, review what I had written, and
verify the output against numbers I had stated in advance. I wanted to be able
to defend every line, and that means deciding and typing it.

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
