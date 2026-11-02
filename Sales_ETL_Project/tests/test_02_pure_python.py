"""LEVEL 2 - Pure Python tests and @pytest.mark.parametrize.

No `spark` fixture -> no JVM -> these run in milliseconds. Pull logic out of Spark
into plain functions whenever you can; it is the cheapest code to test.
parametrize runs the same test once per row and reports each case separately.
"""
import pytest

from sales_etl.transforms import fiscal_quarter


@pytest.mark.parametrize(
    "month, expected",
    [
        (2, 1),   # first month of the fiscal year
        (4, 1),   # last month of Q1
        (5, 2),
        (12, 4),
        (1, 4),   # January wraps into the previous fiscal year's Q4
    ],
)
def test_fiscal_quarter_default_feb_start(month, expected):
    assert fiscal_quarter(month) == expected


def test_fiscal_quarter_calendar_year():
    assert [fiscal_quarter(m, fy_start_month=1) for m in (1, 3, 4, 12)] == [1, 1, 2, 4]


@pytest.mark.parametrize("bad_month", [0, 13, -1])
def test_fiscal_quarter_rejects_bad_month(bad_month):
    with pytest.raises(ValueError, match="month must be 1-12"):
        fiscal_quarter(bad_month)
