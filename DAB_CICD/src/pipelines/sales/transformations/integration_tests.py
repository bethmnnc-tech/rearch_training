"""METHOD 1 - Integration tests as SDP expectations (dev + uat/stage only).

Same pipeline, same code in every environment. When the pipeline configuration
`run_integration_tests` is "true" (set per target in
resources/variables/integration_test_variables.yml), these test materialized
views are added to the graph. Dev and uat ingest STATIC test files, so the
expected results are known. Any `expect_or_fail` violation FAILS the pipeline
update, which fails the job task, which fails `databricks bundle run` in CI.
In prod the flag is "false": the test views are never created.
"""
from pyspark import pipelines as dp

RUN_TESTS = spark.conf.get("run_integration_tests", "false").lower() == "true"
EXPECTED_ORDERS = int(spark.conf.get("expected_order_count", "-1"))
ALLOWED_STATUS = "('NEW', 'SHIPPED', 'DELIVERED', 'CANCELLED')"

if RUN_TESTS:

    @dp.materialized_view(name="test_silver_orders_row_count", comment="TEST: known row count from static data")
    @dp.expect_or_fail("row_count_matches_static_data", f"silver_rows = {EXPECTED_ORDERS}")
    def test_silver_orders_row_count():
        return spark.sql("SELECT COUNT(*) AS silver_rows FROM silver_orders")

    @dp.materialized_view(name="test_silver_orders_no_rows_lost", comment="TEST: dedupe keeps one row per valid order")
    @dp.expect_or_fail("one_row_per_bronze_order", "silver_rows = bronze_orders")
    def test_silver_orders_no_rows_lost():
        return spark.sql("""
            SELECT
              (SELECT COUNT(*) FROM silver_orders) AS silver_rows,
              (SELECT COUNT(DISTINCT order_id) FROM bronze_orders
                WHERE order_id IS NOT NULL AND customer_id IS NOT NULL) AS bronze_orders""")

    @dp.materialized_view(name="test_silver_orders_pk_unique", comment="TEST: primary key is unique")
    @dp.expect_or_fail("unique_order_id", "num_entries = 1")
    def test_silver_orders_pk_unique():
        return spark.read.table("silver_orders").groupBy("order_id").count().withColumnRenamed("count", "num_entries")

    @dp.materialized_view(name="test_silver_orders_values", comment="TEST: distinct values and ranges")
    @dp.expect_all_or_fail({
        "status_is_known": f"status IN {ALLOWED_STATUS}",
        "amount_in_range": "amount BETWEEN 0 AND 100000",
        "keys_not_null": "order_id IS NOT NULL AND customer_id IS NOT NULL",
    })
    def test_silver_orders_values():
        return spark.read.table("silver_orders").select("order_id", "customer_id", "status", "amount")

    @dp.materialized_view(name="test_silver_orders_no_orphans", comment="TEST: every order has a customer")
    @dp.expect_or_fail("customer_exists", "c_customer_id IS NOT NULL")
    def test_silver_orders_no_orphans():
        return spark.sql("""
            SELECT o.order_id, c.customer_id AS c_customer_id
            FROM silver_orders o LEFT JOIN silver_customers c ON o.customer_id = c.customer_id""")

    @dp.materialized_view(name="test_gold_reconciles_to_silver", comment="TEST: gold revenue = silver amount")
    @dp.expect_or_fail("revenue_reconciles", "abs(gold_total - silver_total) <= 0.01")
    def test_gold_reconciles_to_silver():
        return spark.sql("""
            SELECT
              (SELECT COALESCE(SUM(revenue), 0) FROM gold_daily_sales) AS gold_total,
              (SELECT COALESCE(SUM(amount), 0) FROM silver_orders) AS silver_total""")
