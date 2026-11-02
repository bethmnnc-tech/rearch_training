"""Data quality helpers: null keys, duplicate keys and orphaned (unmatched) rows.

Null key  = the row's own key (or foreign key) is NULL, so it can never be matched.
Orphan    = the foreign key is filled in, but no parent row has that key.
Keep the two apart: they have different root causes and different fixes.
"""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    """Raised by gate functions so a job task fails loudly."""


def _any_null(keys: list[str]):
    return reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys])


def null_key_rows(df: DataFrame, keys: list[str]) -> DataFrame:
    """Rows where ANY key column is NULL (works for single and composite keys)."""
    return df.filter(_any_null(keys))


def duplicate_keys(df: DataFrame, keys: list[str]) -> DataFrame:
    """Key values that appear more than once, with their count."""
    return df.groupBy(*keys).count().filter("count > 1")


def orphan_rows(child: DataFrame, parent: DataFrame, child_keys: list[str], parent_keys: list[str] | None = None) -> DataFrame:
    """Child rows whose (non-null) foreign key has no match in the parent.

    Rows with a NULL foreign key are NOT orphans here; find them with null_key_rows.
    A left_anti join returns child rows with no match and keeps only child columns.
    """
    parent_keys = parent_keys or child_keys
    cond = [child[c] == parent[p] for c, p in zip(child_keys, parent_keys)]
    return child.filter(~_any_null(child_keys)).join(parent, cond, "left_anti")


def split_valid_quarantine(orders: DataFrame, customers: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Route each order to valid or quarantine (with a reason). Every input row lands in exactly one."""
    null_keys = null_key_rows(orders, ["order_id", "customer_id"]).withColumn(
        "dq_reason", F.lit("NULL_KEY")
    )
    orphans = orphan_rows(orders.filter(F.col("order_id").isNotNull()), customers, ["customer_id"]).withColumn(
        "dq_reason", F.lit("ORPHAN_CUSTOMER")
    )
    quarantine = null_keys.unionByName(orphans)
    valid = orders.filter(~_any_null(["order_id", "customer_id"])).join(
        customers.select("customer_id"), "customer_id", "left_semi"
    )
    return valid, quarantine


def assert_no_orphans(child: DataFrame, parent: DataFrame, child_keys: list[str], max_allowed: int = 0) -> None:
    """Gate for a job task: fail if orphan count is over the threshold."""
    n = orphan_rows(child, parent, child_keys).count()
    if n > max_allowed:
        raise DataQualityError(f"{n} orphan rows on {child_keys} (max allowed {max_allowed})")
