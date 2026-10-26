"""LEVEL 8 - Integration test on Databricks (Delta + Unity Catalog).

Marked `integration`: CI unit stage skips it (pytest -m "not integration").
It runs AFTER `databricks bundle deploy -t dev`, inside the deployed
`unit_tests` job (see resources/test_jobs/test_jobs.yml), against a
throwaway schema. Never point it at real data.
"""
import os
import uuid

import pytest
from pyspark.testing import assertDataFrameEqual

from sales_etl.data_quality import split_valid_quarantine

pytestmark = pytest.mark.integration

CATALOG = os.environ.get("TEST_CATALOG")


@pytest.fixture
def temp_schema(spark):
    if not CATALOG:
        pytest.skip("TEST_CATALOG not set; integration tests run on Databricks only")
    schema = f"{CATALOG}.ut_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    yield schema
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")  # always clean up


def test_quarantine_round_trip_through_delta(spark, temp_schema, orders, customers):
    valid, quarantine = split_valid_quarantine(orders, customers)
    valid.write.mode("overwrite").saveAsTable(f"{temp_schema}.orders_valid")
    quarantine.write.mode("overwrite").saveAsTable(f"{temp_schema}.orders_quarantine")

    assertDataFrameEqual(spark.read.table(f"{temp_schema}.orders_valid"), valid)
    assert spark.read.table(f"{temp_schema}.orders_quarantine").count() == 3


def test_merge_upsert_does_not_duplicate_keys(spark, temp_schema):
    target = f"{temp_schema}.customers"
    spark.createDataFrame([("C1", "EAST")], "customer_id string, region string").write.saveAsTable(target)
    spark.createDataFrame(
        [("C1", "WEST"), ("C2", "EAST")], "customer_id string, region string"
    ).createOrReplaceTempView("updates")

    spark.sql(f"""
        MERGE INTO {target} t USING updates s ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")

    assertDataFrameEqual(spark.read.table(target), [("C1", "WEST"), ("C2", "EAST")])
