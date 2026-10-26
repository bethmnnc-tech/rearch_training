"""Reusable integration checks for Lakeflow Job test tasks (Method 2).

Each check returns None when it passes and raises AssertionError with a clear
message when it fails. `run_checks` runs ALL checks and reports every failure
at once, so one job run tells you everything that is wrong, not just the first.
These functions are themselves unit tested in tests/test_09_integration_checks.py.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_etl.data_quality import duplicate_keys, null_key_rows, orphan_rows


def check_table_exists(spark, table_name: str) -> None:
    assert spark.catalog.tableExists(table_name), f"{table_name} does not exist"


def check_columns(df: DataFrame, expected: list[str]) -> None:
    missing = [c for c in expected if c not in df.columns]
    assert not missing, f"missing columns {missing}"


def check_row_count(df: DataFrame, expected: int | None = None, min_rows: int | None = None) -> None:
    n = df.count()
    if expected is not None:
        assert n == expected, f"expected {expected} rows, found {n}"
    if min_rows is not None:
        assert n >= min_rows, f"expected at least {min_rows} rows, found {n}"


def check_distinct_values(df: DataFrame, col: str, allowed: set) -> None:
    found = {r[0] for r in df.select(col).distinct().collect()}
    unexpected = found - set(allowed)
    assert not unexpected, f"{col} has unexpected values {sorted(map(str, unexpected))}"


def check_range(df: DataFrame, col: str, lo=None, hi=None) -> None:
    cond = F.lit(False)
    if lo is not None:
        cond = cond | (F.col(col) < lo)
    if hi is not None:
        cond = cond | (F.col(col) > hi)
    n = df.filter(cond).count()
    assert n == 0, f"{n} rows with {col} outside [{lo}, {hi}]"


def check_no_duplicates(df: DataFrame, keys: list[str]) -> None:
    n = duplicate_keys(df, keys).count()
    assert n == 0, f"{n} duplicate key values on {keys}"


def check_no_null_keys(df: DataFrame, keys: list[str]) -> None:
    n = null_key_rows(df, keys).count()
    assert n == 0, f"{n} rows with NULL in {keys}"


def check_no_orphans(child: DataFrame, parent: DataFrame, child_keys: list[str], parent_keys: list[str] | None = None) -> None:
    n = orphan_rows(child, parent, child_keys, parent_keys).count()
    assert n == 0, f"{n} orphan rows on {child_keys}"


def check_totals_match(a: DataFrame, a_col: str, b: DataFrame, b_col: str, tol: float = 0.01) -> None:
    """Reconciliation: e.g. gold revenue total == silver amount total."""
    ta = a.agg(F.sum(a_col)).first()[0] or 0
    tb = b.agg(F.sum(b_col)).first()[0] or 0
    assert abs(float(ta) - float(tb)) <= tol, f"totals differ: {a_col}={ta} vs {b_col}={tb}"


def run_checks(checks: list[tuple[str, callable]]) -> list[str]:
    """Run every (name, zero-arg callable). Raise once, listing all failures."""
    failures, passed = [], []
    for name, fn in checks:
        try:
            fn()
            passed.append(name)
        except AssertionError as e:
            failures.append(f"FAIL {name}: {e}")
    for name in passed:
        print(f"PASS {name}")
    for f in failures:
        print(f)
    if failures:
        raise AssertionError(f"{len(failures)} of {len(checks)} integration checks failed:\n" + "\n".join(failures))
    return passed
