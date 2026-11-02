"""Shared SDP code: the SAME file runs in dev, uat (stage) and prod.

bronze_orders comes from bronze_orders.sql (DAB CICD Template). This file adds
customers, silver and gold, built with the unit-tested custom functions in
src/sales_etl. The pipeline's root_path is ../../src, so `sales_etl` imports.
"""
from pyspark import pipelines as dp

from sales_etl.transforms import clean_orders, daily_revenue

SOURCE = spark.conf.get("source_path")


@dp.table(comment="Raw customers, incremental from JSON files")
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{SOURCE}/customers")
    )


@dp.materialized_view(comment="One row per customer")
def silver_customers():
    return spark.read.table("bronze_customers").dropDuplicates(["customer_id"]).select("customer_id", "region")


@dp.materialized_view(comment="Latest version of each order; rows with NULL keys dropped")
@dp.expect_all_or_drop({"order_id_not_null": "order_id IS NOT NULL", "customer_id_not_null": "customer_id IS NOT NULL"})
def silver_orders():
    return clean_orders(spark.read.table("bronze_orders"))


@dp.materialized_view(comment="Daily revenue for BI")
def gold_daily_sales():
    return daily_revenue(spark.read.table("silver_orders"))
