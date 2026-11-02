"""Custom transformation functions for the sales_etl bundle.

Rule of thumb: every function takes DataFrames (or plain values) in and returns
a DataFrame (or plain value) out. No reads, no writes, no `spark` global.
That is what makes them unit-testable with tiny in-memory data.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# --- Level 1: the example from "Introduction to unit tests for PySpark" ----------
def add_new_col(df: DataFrame, new: str, s_col: str) -> DataFrame:
    """0 -> 'Normal', anything else (including null) -> 'Unknown'."""
    return df.withColumn(new, F.when(F.col(s_col) == 0, "Normal").otherwise("Unknown"))


# --- Level 2: plain Python helper (no Spark needed to test it) ------------------
def fiscal_quarter(month: int, fy_start_month: int = 2) -> int:
    """Fiscal quarter (1-4) for a calendar month. Default fiscal year starts in February."""
    if not 1 <= month <= 12:
        raise ValueError(f"month must be 1-12, got {month}")
    return ((month - fy_start_month) % 12) // 3 + 1


# --- Level 3/4: cleaning, business rules, edge cases ----------------------------
def clean_orders(df: DataFrame) -> DataFrame:
    """Trim/upper-case text, cast amount to decimal, keep the latest version of each order."""
    latest_first = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc(), F.col("amount").desc()  # tie-breaker keeps it deterministic
    )
    return (
        df.withColumn("customer_id", F.trim("customer_id"))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
        .withColumn("_rn", F.row_number().over(latest_first))
        .filter("_rn = 1")
        .drop("_rn")
    )


def add_order_size(df: DataFrame, amount_col: str = "amount") -> DataFrame:
    """small < 100 <= medium < 1000 <= large; null -> unknown."""
    amt = F.col(amount_col)
    return df.withColumn(
        "order_size",
        F.when(amt.isNull(), "unknown")
        .when(amt >= 1000, "large")
        .when(amt >= 100, "medium")
        .otherwise("small"),
    )


# --- Level 6: joins and aggregations (gold logic) -------------------------------
def enrich_orders(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """Left join so no order is lost; unmatched customers get region UNKNOWN."""
    return (
        orders.join(customers.select("customer_id", "region"), "customer_id", "left")
        .withColumn("region", F.coalesce("region", F.lit("UNKNOWN")))
    )


def daily_revenue(orders: DataFrame) -> DataFrame:
    return orders.groupBy("order_date").agg(
        F.sum("amount").alias("revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.round(F.avg("amount"), 4).cast("double").alias("avg_order_value"),
    )


# --- Level 7: guard rails and I/O seams -----------------------------------------
def require_columns(df: DataFrame, required: list[str]) -> DataFrame:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df


def load_and_clean_orders(spark, table_name: str) -> DataFrame:
    """Thin I/O wrapper. Tests replace spark.read.table with a fake (see test_07)."""
    raw = spark.read.table(table_name)
    return clean_orders(require_columns(raw, ["order_id", "customer_id", "amount", "status", "updated_at"]))
