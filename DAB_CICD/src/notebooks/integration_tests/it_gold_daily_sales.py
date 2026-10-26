# Databricks notebook source
# METHOD 2 - Integration test task: gold layer (runs in parallel with it_silver_orders).

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..")))  # -> src/

from sales_etl.integration_checks import (
    check_columns,
    check_no_duplicates,
    check_range,
    check_row_count,
    check_table_exists,
    check_totals_match,
    run_checks,
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
gold_t = f"{catalog}.{schema}.gold_daily_sales"

# COMMAND ----------
gold = spark.read.table(gold_t)
silver = spark.read.table(f"{catalog}.{schema}.silver_orders")

run_checks([
    ("gold_daily_sales exists", lambda: check_table_exists(spark, gold_t)),
    ("expected columns", lambda: check_columns(gold, ["order_date", "revenue", "order_count", "avg_order_value"])),
    ("has rows", lambda: check_row_count(gold, min_rows=1)),
    ("one row per day", lambda: check_no_duplicates(gold, ["order_date"])),
    ("no negative revenue", lambda: check_range(gold, "revenue", lo=0)),
    ("revenue reconciles to silver", lambda: check_totals_match(gold, "revenue", silver, "amount")),
])
