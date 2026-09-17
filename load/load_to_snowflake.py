"""
Load the homework.csv order extract into the Snowflake raw layer.

Strategy: full-snapshot reload. The source is a complete extract of all
orders, not a delta feed, so every run replaces the contents of
RAW.ORDERS_RAW rather than merging into it. This makes the load idempotent
by construction.

Two-step load, because write_pandas cannot write a VARIANT column from a
Python string:
  1. write_pandas() lands the frame into a transient STRING-typed staging table
  2. INSERT ... SELECT PARSE_JSON(order_items_raw) casts it into ORDERS_RAW

The staging write happens BEFORE ORDERS_RAW is truncated, so a failed run
never leaves the raw table empty or half-loaded.
"""

import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

# Load .env before the configuration constants below read the environment.
# A no-op in production, where the orchestrator injects the variables.
load_dotenv()

# Configuration

RAW_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "FASTMARKETS")
RAW_SCHEMA = os.environ.get("SNOWFLAKE_RAW_SCHEMA", "RAW")

RAW_TABLE = "ORDERS_RAW"
STAGING_TABLE = "_ORDERS_RAW_STAGING"

# Credentials and connection context. Validated before connecting so the
# script fails with a clear message instead of an opaque connector error.
REQUIRED_ENV_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
)

# The source columns, in the order they appear in homework.csv. Used both to
# validate the download and as the input to _source_row_hash.
BUSINESS_COLUMNS = (
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "order_id",
    "order_date",
    "order_total",
    "order_items",
)

# Network timeout for the source fetch: (connect, read) in seconds.
HTTP_TIMEOUT = (10, 60)

log = logging.getLogger("load_to_snowflake")


def fetch_csv(source_url: str) -> pd.DataFrame:
    """Download the source extract and parse it into a DataFrame"""
    log.info("Fetching source extract from %s", source_url)

    response = requests.get(source_url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    response.encoding = "utf-8"

    #   dtype=str             no type inference; it would strip leading zeros
    #                         from ids and guess at order_items.

    #   keep_default_na=False keep "N/A" and "" as the literal strings the
    #                         source sent. Converting them to NaN would erase
    #                         the difference between "the source said N/A" and
    #                         "the source said nothing" - and that distinction
    #                         is exactly what staging flags.
    df = pd.read_csv(StringIO(response.text), dtype=str, keep_default_na=False)

    missing = set(BUSINESS_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Source extract is missing columns: {sorted(missing)}")

    if df.empty:
        raise ValueError("Source extract parsed to zero rows; refusing to load")

    log.info("Fetched %d rows, %d columns", len(df), len(df.columns))

    return df[list(BUSINESS_COLUMNS)]


def df_to_variant_ready(df: pd.DataFrame) -> pd.DataFrame:
    """ Add audit columns and shape the frame for this string staging table """
    out = df.copy()

    # Joined with \x1f (ASCII unit separator) rather than a comma or pipe: the
    # delimiter must be a character that cannot occur inside a value, or two
    # different rows could hash to the same digest.
    hash_input = out[list(BUSINESS_COLUMNS)].apply(
        lambda row: "\x1f".join(row), axis=1
    )
    out["_source_row_hash"] = hash_input.map(
        lambda text: hashlib.sha256(text.encode("utf-8")).hexdigest()
    )

    out["_loaded_at"] = datetime.now(timezone.utc)

    # order_items stays a plain string here; PARSE_JSON casts it into VARIANT
    # on the Snowflake side, because write_pandas cannot write VARIANT.
    out = out.rename(columns={"order_items": "order_items_raw"}, errors="raise")

    # write_pandas quotes identifiers by default, so the DataFrame column
    # names must match the table's stored case. Unquoted Snowflake
    # identifiers fold to uppercase.
    out.columns = [column.upper() for column in out.columns]
    return out

# SQL

_FQ_RAW = f"{RAW_DATABASE}.{RAW_SCHEMA}.{RAW_TABLE}"
_FQ_STAGING = f"{RAW_DATABASE}.{RAW_SCHEMA}.{STAGING_TABLE}"

CREATE_SCHEMA_SQL = f"CREATE SCHEMA IF NOT EXISTS {RAW_DATABASE}.{RAW_SCHEMA}"

# TRANSIENT: this table lives for the duration of one run, so there is no
# reason to pay for fail-safe storage on it. order_items_raw is STRING because
# write_pandas cannot populate a VARIANT column from a Python string.
CREATE_STAGING_TABLE_SQL = f"""
CREATE OR REPLACE TRANSIENT TABLE {_FQ_STAGING} (
    customer_id         STRING,
    customer_name       STRING,
    customer_phone      STRING,
    customer_email      STRING,
    order_id            STRING,
    order_date          STRING,
    order_total         STRING,
    order_items_raw     STRING,
    _source_row_hash    STRING,
    _loaded_at          TIMESTAMP_TZ
)
"""

# Everything stays STRING except order_items: the raw layer mirrors what
# arrived, and typing is a staging-model decision. IF NOT EXISTS rather than
# OR REPLACE, so a rerun never drops the object itself.
CREATE_RAW_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {_FQ_RAW} (
    customer_id         STRING,
    customer_name       STRING,
    customer_phone      STRING,
    customer_email      STRING,
    order_id            STRING,
    order_date          STRING,
    order_total         STRING,
    order_items         VARIANT,
    _source_row_hash    STRING,
    _loaded_at          TIMESTAMP_TZ
)
"""

# INSERT OVERWRITE replaces the table contents in a single atomic statement.
# A separate TRUNCATE then INSERT would leave a window in which the raw table
# is empty, and TRUNCATE is DDL in Snowflake so it cannot be wrapped in one
# transaction with the INSERT.
INSERT_RAW_FROM_STAGING_SQL = f"""
INSERT OVERWRITE INTO {_FQ_RAW} (
    customer_id, customer_name, customer_phone, customer_email,
    order_id, order_date, order_total, order_items,
    _source_row_hash, _loaded_at
)
SELECT
    customer_id, customer_name, customer_phone, customer_email,
    order_id, order_date, order_total, PARSE_JSON(order_items_raw),
    _source_row_hash, _loaded_at
FROM {_FQ_STAGING}
"""

DROP_STAGING_TABLE_SQL = f"DROP TABLE IF EXISTS {_FQ_STAGING}"


# Load


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Open a Snowflake connection from environment variables."""
    missing = [name for name in REQUIRED_ENV_VARS if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=RAW_DATABASE,
        schema=RAW_SCHEMA,
        # Fail fast rather than retrying a bad credential for minutes.
        login_timeout=30,
    )


def load(conn: snowflake.connector.SnowflakeConnection, df: pd.DataFrame) -> int:
    """Land the frame in staging, then swap it into the raw table.

    The staging table is dropped on success only. If anything fails it is
    left in place: it holds the un-parsed order_items strings and is the
    only thing that can show which row PARSE_JSON choked on.
    """

    cursor = conn.cursor()
    try:
        cursor.execute(CREATE_SCHEMA_SQL)
        cursor.execute(CREATE_RAW_TABLE_SQL)
        cursor.execute(CREATE_STAGING_TABLE_SQL)

        success, n_chunks, n_rows, _ = write_pandas(
            conn,
            df,
            table_name=STAGING_TABLE,
            database=RAW_DATABASE,
            schema=RAW_SCHEMA,
        )
        if not success:
            raise RuntimeError("write_pandas failed writing the staging table")
        log.info("Staged %d rows in %d chunk(s)", n_rows, n_chunks)

        # Guard against a partial upload being swapped in as a full snapshot.
        if n_rows != len(df):
            raise RuntimeError(f"Staged {n_rows} rows, expected {len(df)}")

        cursor.execute(INSERT_RAW_FROM_STAGING_SQL)
        inserted = cursor.rowcount
        log.info("Loaded %d rows into %s", inserted, _FQ_RAW)

        cursor.execute(DROP_STAGING_TABLE_SQL)
        return inserted
    except Exception:
        log.error(
            "Load failed; staging table %s left in place for inspection",
            _FQ_STAGING,
        )
        raise
    finally:
        cursor.close()


# Entry point


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load the homework.csv order extract into Snowflake."
    )
    parser.add_argument(
        "--source-url",
        default=os.environ.get("SOURCE_CSV_URL"),
        help="HTTP(S) URL of the source CSV. Defaults to $SOURCE_CSV_URL.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    # Exit 2 for a usage problem, 1 for a runtime failure, 0 for success.
    if not args.source_url:
        log.error("No source URL: pass --source-url or set SOURCE_CSV_URL")
        return 2

    try:
        frame = df_to_variant_ready(fetch_csv(args.source_url))
        conn = get_connection()
        try:
            load(conn, frame)
        finally:
            conn.close()
    except Exception as exc:
        # Operators get the message; the traceback is there at DEBUG level.
        log.error("Load failed: %s", exc)
        log.debug("Traceback:", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
