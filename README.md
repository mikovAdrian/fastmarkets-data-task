# Fastmarkets Data Engineer Take-Home

A Python extract-and-load script that pulls a CSV order extract over HTTP and
lands it in a Snowflake raw layer, with the nested `order_items` array stored
as `VARIANT` so it stays queryable in place.

## Stack

Python 3.14 · snowflake-connector-python · pandas · Snowflake

## Repository layout

    load/          the extract-and-load script and its pinned dependencies
    dbt_project/   dbt project scaffolding (models not yet written)
    data/          local working directory for the extract (gitignored)

## Setup

    python3 -m venv venv
    source venv/bin/activate
    pip install -r load/requirements.txt

Then copy the environment template and fill it in:

    cp .env.example .env

`.env` is gitignored and never committed. The script reads plain environment
variables and calls `load_dotenv()` only as a local convenience, so in
production the same contract is satisfied by an orchestrator's secret backend
with no `.env` file present.

Required: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
`SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`. Optional with defaults:
`SNOWFLAKE_DATABASE` (`FASTMARKETS`), `SNOWFLAKE_RAW_SCHEMA` (`RAW`).

## Running it

    python load/load_to_snowflake.py --source-url https://.../homework.csv

The URL can also come from `SOURCE_CSV_URL`, in which case the flag is
optional. `--log-level` takes `DEBUG`, `INFO`, `WARNING` or `ERROR`.

Exit codes: `0` success, `1` runtime failure, `2` usage problem such as a
missing source URL. The distinction matters to a scheduler deciding whether a
retry is worth attempting.

## What it produces

`RAW.ORDERS_RAW`, one row per source CSV row. Every business column is
`STRING` except `order_items`, which is `VARIANT`. The raw layer mirrors what
arrived; typing and validation are staging-model concerns, not load concerns.

Two audit columns are added:

| Column | Purpose |
| --- | --- |
| `_loaded_at` | UTC load timestamp |
| `_source_row_hash` | SHA-256 of the business columns, so monitoring can detect the same `order_id` arriving with different content between runs |

## Load design

**Full-snapshot reload.** The source is a complete extract of all orders, not
a delta feed, so each run replaces the table contents. That makes the load
idempotent by construction: running it twice leaves the same state as running
it once. An incremental version would `MERGE` on `order_id` or keep a
`_loaded_at` high-watermark; `_source_row_hash` already makes the change
detection for that cheap.

**Two-step load.** `write_pandas` cannot write a `VARIANT` column from a
Python string, so the frame first lands in a transient staging table with
`order_items_raw` typed as `STRING`, then a single
`INSERT OVERWRITE ... SELECT PARSE_JSON(order_items_raw)` casts it into the
real table. `write_pandas` itself does a `PUT` and `COPY INTO` against a
temporary internal stage, so this is a convenience wrapper rather than a
different mechanism.

**`INSERT OVERWRITE`, not `TRUNCATE` then `INSERT`.** The overwrite replaces
the contents in one atomic statement. Two separate statements would leave a
window in which the raw table is empty, and `TRUNCATE` is DDL in Snowflake so
it cannot be wrapped in a transaction with the insert.

**The staging table is dropped on success only.** If anything fails it is
left in place, because it holds the un-parsed `order_items` strings and is
the only thing that can show which row `PARSE_JSON` rejected. That is also
why it is `TRANSIENT` rather than `TEMPORARY`: a temporary table would vanish
with the session, including on the failure you need to debug.

## Handling dirty input

The sample extract carries known defects: `customer_phone` values wrapped in
literal double-quote characters from a source double-escaping bug, and
placeholder junk (`N/A`, `invalid-email`, `123456`, empty strings) mixed in
with valid contact details.

The loader deliberately does not clean any of it. `read_csv` runs with
`dtype=str` and `keep_default_na=False`, so no type inference occurs and
`N/A` stays the literal string `N/A` rather than becoming `NaN`. Converting
placeholders to nulls at read time would erase the difference between "the
source sent a placeholder" and "the source sent nothing" — two different
upstream defects with different fixes. Validation and flagging belong in
staging, where the raw value can stay visible next to the flag.

The loader does fail fast on structural problems: a non-2xx response, a
missing expected column, or an extract that parses to zero rows. The last one
matters because this load replaces the table contents, and an empty extract
must not be allowed to replace a good snapshot with nothing.

## Next steps

- dbt staging and mart models over `RAW.ORDERS_RAW`
- Stage-based loading (`PUT` + `COPY INTO` from an external stage with
  `ON_ERROR = CONTINUE` and wildcard file patterns) for when the source
  becomes continuous cloud-storage drops instead of one small HTTP file
- Orchestration as a Dagster op or Airflow task, with `dbt build` downstream

## Use of AI

TODO — your own account of how AI was used, in your words.

## Reviewer access

TODO — dedicated read-only Snowflake role and user, not `ACCOUNTADMIN`.