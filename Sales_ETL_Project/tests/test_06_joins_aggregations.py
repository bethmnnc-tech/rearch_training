"""LEVEL 6 - Gold logic: aggregations and floating-point tolerance.

Aggregates hide mistakes (double counting from a bad join, wrong grain).
Hand-calculate the expected numbers from a few rows so the test is obviously right.
"""
from datetime import date

from pyspark.testing import assertDataFrameEqual

from sales_etl.transforms import daily_revenue


def test_daily_revenue(spark):
    orders = spark.createDataFrame(
        [
            (1, date(2026, 10, 1), 10.0),
            (2, date(2026, 10, 1), 20.0),
            (2, date(2026, 10, 1), 0.0),   # 2nd line of order 2: count orders DISTINCT
            (3, date(2026, 10, 2), 5.0),
        ],
        "order_id int, order_date date, amount double",
    )
    expected = spark.createDataFrame(
        [
            (date(2026, 10, 1), 30.0, 2, 10.0),   # avg = 30 / 3 rows
            (date(2026, 10, 2), 5.0, 1, 5.0),
        ],
        "order_date date, revenue double, order_count bigint, avg_order_value double",
    )
    assertDataFrameEqual(daily_revenue(orders), expected)


def test_float_tolerance(spark):
    """0.1 + 0.2 != 0.3 in floating point. rtol/atol absorb that noise."""
    actual = spark.createDataFrame([(0.1 + 0.2,)], "x double")
    expected = spark.createDataFrame([(0.3,)], "x double")
    assertDataFrameEqual(actual, expected, rtol=1e-5)


def test_sorted_output_when_order_matters(spark):
    """Only check row order when the function promises it (e.g. it ends with orderBy)."""
    df = spark.createDataFrame([(2,), (1,)], "n int").orderBy("n")
    assertDataFrameEqual(df, [(1,), (2,)], checkRowOrder=True)  # a list of Rows/tuples also works
