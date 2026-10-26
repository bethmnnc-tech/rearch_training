"""LEVEL 7 - Expected failures and replacing I/O with fakes.

- pytest.raises proves bad input fails fast with a clear message.
- Code that reads tables (spark.read.table) can still be unit tested:
  hand it a fake reader that returns an in-memory DataFrame. No catalog,
  no table, no workspace needed.
"""
from datetime import datetime
from types import SimpleNamespace

import pytest

from sales_etl.transforms import load_and_clean_orders, require_columns


def test_require_columns_lists_what_is_missing(spark):
    df = spark.createDataFrame([(1,)], "order_id int")
    with pytest.raises(ValueError, match=r"\['customer_id', 'amount'\]"):
        require_columns(df, ["order_id", "customer_id", "amount"])


def test_load_and_clean_orders_with_fake_reader(spark):
    raw = spark.createDataFrame(
        [(1, " C1", 9.5, "new", datetime(2026, 10, 1))],
        "order_id int, customer_id string, amount double, status string, updated_at timestamp",
    )
    tables_read = []

    def fake_table(name):
        tables_read.append(name)
        return raw

    fake_spark = SimpleNamespace(read=SimpleNamespace(table=fake_table))

    result = load_and_clean_orders(fake_spark, "dev.sales.bronze_orders")

    assert tables_read == ["dev.sales.bronze_orders"]      # read the table we asked for
    assert result.first()["customer_id"] == "C1"           # and the cleaning still ran


def test_load_fails_when_source_schema_drifts(spark):
    drifted = spark.createDataFrame([(1, "C1")], "order_id int, customer_id string")
    fake_spark = SimpleNamespace(read=SimpleNamespace(table=lambda _: drifted))
    with pytest.raises(ValueError, match="Missing required columns"):
        load_and_clean_orders(fake_spark, "any.table")
