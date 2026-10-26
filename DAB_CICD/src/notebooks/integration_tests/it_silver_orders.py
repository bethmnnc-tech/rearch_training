# Databricks notebook source
# METHOD 2 - Integration test task: silver layer.
# Runs AFTER the pipeline task in resources/test_jobs/test_jobs.yml.
# Job parameters (catalog, schema, expected_order_count) arrive as widgets, so the
# same notebook tests dev or uat. A failed check fails this task and the job.

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..")))  # -> src/

from sales_etl.integration_checks import (
    check_columns,
    check_distinct_values,
    check_no_duplicates,
    check_no_null_keys,
    check_no_orphans,
    check_range,
    check_row_count,
    check_table_exists,
    run_checks,
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
expected = int(dbutils.widgets.get("expected_order_count"))
orders_t = f"{catalog}.{schema}.silver_orders"
customers_t = f"{catalog}.{schema}.silver_customers"

# COMMAND ----------
orders = spark.read.table(orders_t)
customers = spark.read.table(customers_t)

run_checks([
    ("silver_orders exists", lambda: check_table_exists(spark, orders_t)),
    ("expected columns", lambda: check_columns(orders, ["order_id", "customer_id", "amount", "status", "updated_at"])),
    ("row count = static data", lambda: check_row_count(orders, expected=expected)),
    ("order_id unique", lambda: check_no_duplicates(orders, ["order_id"])),
    ("no NULL keys", lambda: check_no_null_keys(orders, ["order_id", "customer_id"])),
    ("no orphan customers", lambda: check_no_orphans(orders, customers, ["customer_id"])),
    ("status values known", lambda: check_distinct_values(orders, "status", {"NEW", "SHIPPED", "DELIVERED", "CANCELLED"})),
    ("amount in range", lambda: check_range(orders, "amount", lo=0, hi=100_000)),
])
