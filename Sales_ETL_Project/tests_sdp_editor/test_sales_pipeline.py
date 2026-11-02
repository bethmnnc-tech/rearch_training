"""LEVEL 10 (Beta) - Unit testing a Lakeflow Spark Declarative Pipeline.

Status: Beta (docs updated Oct 12, 2026). Requires Databricks Runtime 18.1+,
a TRIGGERED pipeline, and that YOU are the pipeline Owner with USE CATALOG +
CREATE SCHEMA on its default catalog.

Runs ONLY from the Lakeflow Pipelines Editor (+ > Test). It does not run from
pytest on a CI runner or over Spark Connect, so keep it out of `testpaths`.
Writes are redirected to a temporary schema by TABLE NAME only: anything read or
written by path (/Volumes, s3://) or through Auto Loader/Kafka is NOT isolated.

Example pipeline under test (src/pipelines/sales/transformations):
    bronze_orders  -> silver_orders  (@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL"))
                   -> orders_orphans (orders whose customer_id is missing from silver_customers)
Replace `main.sales` with your pipeline's catalog.schema.
"""
from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()
T = "main.sales"


def _mock_inputs():
    test_spark.createDataFrame(
        [(1, "C1", 120.0), (2, None, 50.0), (3, "C9", 75.0)],
        "order_id int, customer_id string, amount double",
    ).write.saveAsTable(f"{T}.bronze_orders")
    test_spark.createDataFrame([("C1", "EAST")], "customer_id string, region string").write.saveAsTable(
        f"{T}.silver_customers"
    )


def test_null_customer_dropped_by_expectation():
    _mock_inputs()
    status = test_pipeline.run(test_spark, {f"{T}.silver_orders"})   # run only this table

    assert status.is_success
    ids = [r.order_id for r in test_spark.read.table(f"{T}.silver_orders").collect()]
    assert 2 not in ids                                               # NULL key dropped
    assert status.flow_info["silver_orders"].records_written == 2


def test_orphans_detected():
    _mock_inputs()
    status = test_pipeline.run(test_spark, {f"{T}.silver_orders", f"{T}.orders_orphans"})

    assert status.is_success
    assert [r.order_id for r in test_spark.read.table(f"{T}.orders_orphans").collect()] == [3]
