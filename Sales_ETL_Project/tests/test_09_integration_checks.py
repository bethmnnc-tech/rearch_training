"""LEVEL 9 - Test the integration checks themselves.

The Job-based integration tests (Method 2) call sales_etl.integration_checks.
A check that never fails is worse than no check, so each one gets a passing
case AND a failing case here, on tiny local data, before it runs in dev/stage.
"""
import pytest

from sales_etl.integration_checks import (
    check_columns,
    check_distinct_values,
    check_no_duplicates,
    check_no_null_keys,
    check_no_orphans,
    check_range,
    check_row_count,
    check_totals_match,
    run_checks,
)


@pytest.fixture
def silver(spark):
    return spark.createDataFrame(
        [(1, "C1", 120.0, "NEW"), (2, "C2", 50.0, "SHIPPED")],
        "order_id int, customer_id string, amount double, status string",
    )


def test_checks_pass_on_good_data(silver, customers):
    check_columns(silver, ["order_id", "amount"])
    check_row_count(silver, expected=2)
    check_distinct_values(silver, "status", {"NEW", "SHIPPED", "DELIVERED"})
    check_range(silver, "amount", lo=0, hi=10_000)
    check_no_duplicates(silver, ["order_id"])
    check_no_null_keys(silver, ["order_id", "customer_id"])
    check_no_orphans(silver, customers, ["customer_id"])


@pytest.mark.parametrize(
    "check, message",
    [
        (lambda df, c: check_columns(df, ["order_date"]), "missing columns"),
        (lambda df, c: check_row_count(df, expected=5), "expected 5 rows, found 2"),
        (lambda df, c: check_row_count(df, min_rows=3), "at least 3"),
        (lambda df, c: check_distinct_values(df, "status", {"NEW"}), "unexpected values"),
        (lambda df, c: check_range(df, "amount", lo=100), "outside"),
        (lambda df, c: check_no_duplicates(df.unionByName(df), ["order_id"]), "duplicate"),
        (lambda df, c: check_no_orphans(df, c.filter("customer_id = 'C1'"), ["customer_id"]), "1 orphan"),
    ],
)
def test_each_check_fails_on_bad_data(silver, customers, check, message):
    with pytest.raises(AssertionError, match=message):
        check(silver, customers)


def test_null_key_check_fails(spark):
    df = spark.createDataFrame([(None, "C1")], "order_id int, customer_id string")
    with pytest.raises(AssertionError, match="1 rows with NULL"):
        check_no_null_keys(df, ["order_id"])


def test_totals_match_reconciliation(spark, silver):
    gold = spark.createDataFrame([(170.0,)], "revenue double")
    check_totals_match(gold, "revenue", silver, "amount")
    with pytest.raises(AssertionError, match="totals differ"):
        check_totals_match(gold, "revenue", silver.limit(1), "amount")


def test_run_checks_reports_every_failure(silver):
    checks = [
        ("rows", lambda: check_row_count(silver, expected=2)),
        ("too_many", lambda: check_row_count(silver, expected=9)),
        ("range", lambda: check_range(silver, "amount", hi=100)),
    ]
    with pytest.raises(AssertionError) as exc:
        run_checks(checks)
    assert "2 of 3 integration checks failed" in str(exc.value)
    assert "FAIL too_many" in str(exc.value) and "FAIL range" in str(exc.value)
