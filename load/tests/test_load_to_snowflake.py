"""Unit tests for the extract-and-load script.

Everything here runs against a fake HTTP response and a fake Snowflake
connection, so the suite needs no credentials and no network. That covers the
logic and the statement ordering; it deliberately cannot cover how the driver
serializes types, which is why the timezone bug documented in the README was
only caught by a live run.

    pip install -r load/requirements-dev.txt
    pytest load/tests -q
"""

import sys
from pathlib import Path

import pandas as pd
import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import load_to_snowflake as loader  # noqa: E402


HEADER = (
    "customer_id,customer_name,customer_phone,customer_email,"
    "order_id,order_date,order_total,order_items"
)

# Three quote characters is an escaped literal quote in CSV, which is how the
# source's double-escaping bug actually arrives.
ROWS = (
    'C1,Ann,"""+44-1693623526""",A@X.COM,O1,2024-01-02,300.0,'
    '"[{""product_id"": ""A1"", ""quantity"": 2, ""price"": 150.0}]"\n'
    'C2,Bob,N/A,,O2,2024-01-09,0,"[]"\n'
)
GOOD_CSV = f"{HEADER}\n{ROWS}"


class FakeResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code
        self.encoding = None

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


@pytest.fixture
def serve(monkeypatch):
    """Point requests.get at a canned response."""

    def _serve(text, status_code=200):
        monkeypatch.setattr(
            loader.requests,
            "get",
            lambda url, timeout=None: FakeResponse(text, status_code),
        )

    return _serve


# --- fetch_csv ---------------------------------------------------------------


def test_fetch_returns_declared_columns_in_order(serve):
    serve(GOOD_CSV)
    frame = loader.fetch_csv("http://example/orders.csv")
    assert len(frame) == 2
    assert list(frame.columns) == list(loader.BUSINESS_COLUMNS)


def test_placeholders_survive_as_literal_strings(serve):
    """The whole point of keep_default_na=False.

    "N/A" and "" must stay distinguishable: one is a placeholder written by a
    broken integration, the other is an absent value. Collapsing both to NaN
    loses a distinction the staging models rely on.
    """
    serve(GOOD_CSV)
    frame = loader.fetch_csv("http://example/orders.csv")
    assert frame.loc[1, "customer_phone"] == "N/A"
    assert frame.loc[1, "customer_email"] == ""


def test_no_type_inference(serve):
    serve(GOOD_CSV)
    frame = loader.fetch_csv("http://example/orders.csv")
    assert frame["order_total"].dtype == object
    assert frame.loc[1, "order_total"] == "0"


def test_literal_quote_characters_are_preserved(serve):
    """The loader must not clean the source's escaping bug - staging does."""
    serve(GOOD_CSV)
    frame = loader.fetch_csv("http://example/orders.csv")
    assert frame.loc[0, "customer_phone"] == '"+44-1693623526"'


def test_unexpected_upstream_column_is_dropped(serve):
    serve(f"{HEADER},surprise\n" + ROWS.replace("\n", ",x\n").rstrip(",x\n") + ",x\n")
    frame = loader.fetch_csv("http://example/orders.csv")
    assert "surprise" not in frame.columns


def test_missing_column_raises(serve):
    serve(GOOD_CSV.replace("customer_email,", ""))
    with pytest.raises(ValueError, match="missing columns"):
        loader.fetch_csv("http://example/orders.csv")


def test_empty_extract_is_refused(serve):
    """An empty extract must not be allowed to replace a good snapshot."""
    serve(f"{HEADER}\n")
    with pytest.raises(ValueError, match="zero rows"):
        loader.fetch_csv("http://example/orders.csv")


def test_http_error_raises_before_parsing(serve):
    """A 404 body is valid HTML that read_csv would turn into garbage."""
    serve("<html>Not Found</html>", status_code=404)
    with pytest.raises(requests.HTTPError):
        loader.fetch_csv("http://example/orders.csv")


# --- df_to_variant_ready -----------------------------------------------------


@pytest.fixture
def source_frame(serve):
    serve(GOOD_CSV)
    return loader.fetch_csv("http://example/orders.csv")


def test_input_frame_is_not_mutated(source_frame):
    before = source_frame.copy()
    loader.df_to_variant_ready(source_frame)
    assert source_frame.equals(before)


def test_columns_are_uppercased_for_write_pandas(source_frame):
    out = loader.df_to_variant_ready(source_frame)
    assert all(column == column.upper() for column in out.columns)


def test_order_items_is_renamed_for_the_string_staging_table(source_frame):
    out = loader.df_to_variant_ready(source_frame)
    assert "ORDER_ITEMS_RAW" in out.columns
    assert "ORDER_ITEMS" not in out.columns


def test_hash_is_sha256_and_row_distinct(source_frame):
    out = loader.df_to_variant_ready(source_frame)
    assert out["_SOURCE_ROW_HASH"].str.fullmatch(r"[0-9a-f]{64}").all()
    assert out["_SOURCE_ROW_HASH"].nunique() == len(out)


def test_hash_is_deterministic(source_frame):
    first = loader.df_to_variant_ready(source_frame)["_SOURCE_ROW_HASH"].tolist()
    second = loader.df_to_variant_ready(source_frame)["_SOURCE_ROW_HASH"].tolist()
    assert first == second


def test_hash_changes_when_content_changes(source_frame):
    baseline = loader.df_to_variant_ready(source_frame)["_SOURCE_ROW_HASH"][0]
    edited = source_frame.copy()
    edited.loc[0, "order_total"] = "301.0"
    changed = loader.df_to_variant_ready(edited)["_SOURCE_ROW_HASH"][0]
    assert baseline != changed


def test_loaded_at_is_timezone_aware_utc(source_frame):
    out = loader.df_to_variant_ready(source_frame)
    assert str(out["_LOADED_AT"].dtype).endswith(", UTC]")


# --- get_connection ----------------------------------------------------------


def test_missing_credentials_name_the_variables(monkeypatch):
    for name in loader.REQUIRED_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError) as exc:
        loader.get_connection()
    for name in loader.REQUIRED_ENV_VARS:
        assert name in str(exc.value)


# --- load --------------------------------------------------------------------


class FakeCursor:
    def __init__(self, journal, fail_on=None):
        self.journal = journal
        self.fail_on = fail_on
        self.rowcount = 2

    def execute(self, sql):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("PARSE_JSON error on row 7")
        self.journal.append(sql.strip().split("\n")[0].strip() or sql.strip())

    def close(self):
        self.journal.append("CURSOR CLOSED")


class FakeConnection:
    def __init__(self, fail_on=None):
        self.journal = []
        self._fail_on = fail_on

    def cursor(self):
        return FakeCursor(self.journal, self._fail_on)


def _first_words(journal, count=3):
    return [" ".join(entry.split()[:count]) for entry in journal]


@pytest.fixture
def frame():
    return pd.DataFrame({"A": ["1", "2"]})


def _stub_write_pandas(monkeypatch, success=True, rows=2, captured=None):
    def fake(conn, df, table_name=None, database=None, schema=None, **kwargs):
        if captured is not None:
            captured["table"] = f"{database}.{schema}.{table_name}"
            captured["rows"] = len(df)
        return success, 1, rows, None

    monkeypatch.setattr(loader, "write_pandas", fake)


def test_load_issues_statements_in_order(monkeypatch, frame):
    _stub_write_pandas(monkeypatch)
    conn = FakeConnection()
    assert loader.load(conn, frame) == 2
    assert _first_words(conn.journal) == [
        "CREATE SCHEMA IF",
        "CREATE TABLE IF",
        "CREATE OR REPLACE",
        "INSERT OVERWRITE INTO",
        "DROP TABLE IF",
        "CURSOR CLOSED",
    ]


def test_write_pandas_targets_the_staging_table(monkeypatch, frame):
    captured = {}
    _stub_write_pandas(monkeypatch, captured=captured)
    loader.load(FakeConnection(), frame)
    assert captured["table"] == loader._FQ_STAGING


def test_staging_table_survives_a_failure_for_inspection(monkeypatch, frame):
    """On failure the staging table holds the un-parsed order_items strings."""
    _stub_write_pandas(monkeypatch)
    conn = FakeConnection(fail_on="INSERT OVERWRITE")
    with pytest.raises(RuntimeError, match="PARSE_JSON"):
        loader.load(conn, frame)
    assert not any("DROP TABLE" in entry for entry in conn.journal)
    assert "CURSOR CLOSED" in conn.journal


def test_partial_upload_stops_before_the_swap(monkeypatch, frame):
    """A short staging write must not be swapped in as a full snapshot."""
    _stub_write_pandas(monkeypatch, rows=1)
    conn = FakeConnection()
    with pytest.raises(RuntimeError, match="Staged 1 rows, expected 2"):
        loader.load(conn, frame)
    assert not any("INSERT OVERWRITE" in entry for entry in conn.journal)


def test_write_pandas_failure_is_raised(monkeypatch, frame):
    _stub_write_pandas(monkeypatch, success=False)
    with pytest.raises(RuntimeError, match="write_pandas failed"):
        loader.load(FakeConnection(), frame)


# --- main --------------------------------------------------------------------


def test_main_returns_2_without_a_source_url(monkeypatch):
    monkeypatch.delenv("SOURCE_CSV_URL", raising=False)
    assert loader.main([]) == 2


def test_main_returns_1_on_failure(monkeypatch, serve):
    serve("<html>gone</html>", status_code=500)
    assert loader.main(["--source-url", "http://example/orders.csv"]) == 1


def test_main_returns_0_on_success(monkeypatch, serve):
    serve(GOOD_CSV)
    for name in loader.REQUIRED_ENV_VARS:
        monkeypatch.setenv(name, "x")
    monkeypatch.setattr(loader.snowflake.connector, "connect", lambda **kw: FakeConnection())
    _stub_write_pandas(monkeypatch)
    monkeypatch.setattr(FakeConnection, "close", lambda self: None, raising=False)
    assert loader.main(["--source-url", "http://example/orders.csv"]) == 0
