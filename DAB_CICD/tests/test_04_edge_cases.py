"""LEVEL 4 - Business rules, boundaries and dedupe edge cases.

Test the edges, not just the happy path: exact thresholds, NULLs, blanks,
duplicates and ties. Most production bugs live there.
"""
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.testing import assertDataFrameEqual

from sales_etl.transforms import add_order_size, clean_orders


@pytest.mark.parametrize(
    "amount, expected",
    [
        (99.99, "small"),
        (100.0, "medium"),   # boundary: >= 100
        (999.99, "medium"),
        (1000.0, "large"),   # boundary: >= 1000
        (0.0, "small"),
        (-5.0, "small"),     # refunds: decide the rule and pin it with a test
        (None, "unknown"),
    ],
)
def test_add_order_size_boundaries(spark, amount, expected):
    df = spark.createDataFrame([(amount,)], "amount double")
    assert add_order_size(df).first()["order_size"] == expected


def test_clean_orders_trims_uppercases_and_keeps_latest(spark):
    t1, t2 = datetime(2026, 10, 1, 9), datetime(2026, 10, 1, 10)
    raw = spark.createDataFrame(
        [
            (1, "  C1 ", 10.0, " shipped", t1),   # older version of order 1
            (1, "C1", 12.5, "delivered ", t2),    # latest version of order 1 -> keep
            (2, "C2", 99.999, "new", t1),         # rounds to 100.00 on decimal cast
        ],
        "order_id int, customer_id string, amount double, status string, updated_at timestamp",
    )

    expected = spark.createDataFrame(
        [
            (1, "C1", Decimal("12.50"), "DELIVERED", t2),
            (2, "C2", Decimal("100.00"), "NEW", t1),
        ],
        "order_id int, customer_id string, amount decimal(10,2), status string, updated_at timestamp",
    )
    assertDataFrameEqual(clean_orders(raw), expected)


def test_clean_orders_tie_on_timestamp_is_deterministic(spark):
    """Two versions with the SAME updated_at: the tie-breaker (higher amount) must win every time."""
    t = datetime(2026, 10, 1)
    raw = spark.createDataFrame(
        [(1, "C1", 5.0, "a", t), (1, "C1", 7.0, "b", t)],
        "order_id int, customer_id string, amount double, status string, updated_at timestamp",
    )
    rows = clean_orders(raw).collect()
    assert len(rows) == 1 and rows[0]["amount"] == Decimal("7.00")


def test_clean_orders_empty_input(spark):
    raw = spark.createDataFrame(
        [], "order_id int, customer_id string, amount double, status string, updated_at timestamp"
    )
    assert clean_orders(raw).count() == 0
