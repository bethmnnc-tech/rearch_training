"""LEVEL 5 - Null keys, duplicate keys and orphaned rows (referential integrity).

Null key : the key column is NULL, so the row can never join or MERGE correctly.
Orphan   : the foreign key has a value, but the parent table has no such key.
Both are silent in Spark: an inner join just drops them, and a left join
fills NULLs. These tests prove the code FINDS them instead of losing them.
Sample data comes from the `orders` and `customers` fixtures in conftest.py.
"""
import pytest
from pyspark.testing import assertDataFrameEqual

from sales_etl.data_quality import (
    DataQualityError,
    assert_no_orphans,
    duplicate_keys,
    null_key_rows,
    orphan_rows,
    split_valid_quarantine,
)
from sales_etl.transforms import enrich_orders


# ---------- null keys ----------
def test_null_key_rows_single_key(orders):
    assert sorted(r["amount"] for r in null_key_rows(orders, ["order_id"]).collect()) == [10.0]


def test_null_key_rows_any_of_composite_key(orders):
    # NULL in EITHER column counts: rows 3 (customer_id NULL) and the NULL order_id row
    assert null_key_rows(orders, ["order_id", "customer_id"]).count() == 2


def test_blank_string_key_is_not_null(spark):
    """'' and '  ' are not NULL. Decide whether they should be, then test it.
    Here they are NOT caught, which documents a gap: trim + nullif upstream if needed."""
    df = spark.createDataFrame([("",), ("  ",), (None,)], "customer_id string")
    assert null_key_rows(df, ["customer_id"]).count() == 1


# ---------- duplicate keys ----------
def test_duplicate_keys_found(spark):
    df = spark.createDataFrame([(1,), (1,), (2,), (None,), (None,)], "order_id int")
    expected = spark.createDataFrame([(1, 2), (None, 2)], "order_id int, count bigint")
    # groupBy treats NULLs as one group: duplicate NULL keys show up too
    assertDataFrameEqual(duplicate_keys(df, ["order_id"]), expected)


# ---------- orphans ----------
def test_orphan_rows_finds_unmatched_customer(orders, customers):
    actual = orphan_rows(orders, customers, ["customer_id"])
    expected = orders.filter("order_id = 5")  # C9 does not exist
    assertDataFrameEqual(actual, expected)


def test_null_foreign_key_is_not_reported_as_orphan(orders, customers):
    ids = [r["order_id"] for r in orphan_rows(orders, customers, ["customer_id"]).collect()]
    assert 3 not in ids  # order 3 has customer_id NULL -> null-key check owns it


def test_orphans_on_composite_key(spark):
    order_lines = spark.createDataFrame(
        [(1, 1, "A"), (1, 2, "B"), (2, 1, "C")], "order_id int, line_no int, sku string"
    )
    shipments = spark.createDataFrame(
        [(1, 1, "S1"), (2, 1, "S2"), (2, 9, "S3")],  # (2, 9) has no order line
        "ord_id int, ln int, shipment_id string",
    )
    actual = orphan_rows(shipments, order_lines, ["ord_id", "ln"], ["order_id", "line_no"])
    assert [r["shipment_id"] for r in actual.collect()] == ["S3"]


def test_inner_join_silently_drops_orphans(orders, customers):
    """Why orphan tests matter: an inner join returns FEWER rows and raises no error."""
    joined = orders.join(customers, "customer_id")
    assert joined.count() == 3  # 5 orders in, 3 out: NULL-FK order 3 and orphan order 5 vanish
    assert {3, 5}.isdisjoint({r["order_id"] for r in joined.collect()})


def test_enrich_orders_keeps_orphans_as_unknown(orders, customers):
    enriched = enrich_orders(orders, customers)
    assert enriched.count() == orders.count()                       # nothing lost
    assert enriched.filter("order_id = 5").first()["region"] == "UNKNOWN"


# ---------- routing and gates ----------
def test_split_valid_quarantine_accounts_for_every_row(orders, customers):
    valid, quarantine = split_valid_quarantine(orders, customers)

    assert valid.count() + quarantine.count() == orders.count()  # no row lost or doubled
    assert sorted(r["order_id"] for r in valid.collect()) == [1, 2]
    reasons = {(r["order_id"], r["dq_reason"]) for r in quarantine.collect()}
    assert reasons == {(3, "NULL_KEY"), (None, "NULL_KEY"), (5, "ORPHAN_CUSTOMER")}


def test_assert_no_orphans_raises(orders, customers):
    with pytest.raises(DataQualityError, match="1 orphan rows"):
        assert_no_orphans(orders, customers, ["customer_id"])


def test_assert_no_orphans_respects_threshold(orders, customers):
    assert_no_orphans(orders, customers, ["customer_id"], max_allowed=1)  # no exception


def test_late_arriving_parent_fixes_orphan(spark, orders, customers):
    """Orphans are often just early: once the customer lands, the order is valid."""
    late = customers.unionByName(spark.createDataFrame([("C9", "SOUTH")], customers.schema))
    assert orphan_rows(orders, late, ["customer_id"]).count() == 0
