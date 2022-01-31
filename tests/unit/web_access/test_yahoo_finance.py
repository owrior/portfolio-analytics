import datetime as dt

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture

from pfa.web_access.yahoo_finance import _process_stock_values
from pfa.web_access.yahoo_finance import get_last_business_day


@pytest.mark.parametrize(
    "raw_stock_values, expected_stock_values",
    [
        pytest.param(
            pd.DataFrame(
                [(1, 1, 2, 3, 1), (2, 1, 2, 3, 1)],
                columns=["Date", "m1", "m2", "m3", "stock_id"],
            ),
            pd.DataFrame(
                [
                    (1, 1, "m1", 1),
                    (2, 1, "m1", 1),
                    (1, 1, "m2", 2),
                    (2, 1, "m2", 2),
                    (1, 1, "m3", 3),
                    (2, 1, "m3", 3),
                ],
                columns=["date", "stock_id", "metric", "value"],
            ),
            id="Single stock id",
        ),
        pytest.param(
            pd.DataFrame(
                [(1, 1, 2, 3, 1), (1, 1, 2, 3, 2)],
                columns=["Date", "m1", "m2", "m3", "stock_id"],
            ),
            pd.DataFrame(
                [
                    (1, 1, "m1", 1),
                    (1, 2, "m1", 1),
                    (1, 1, "m2", 2),
                    (1, 2, "m2", 2),
                    (1, 1, "m3", 3),
                    (1, 2, "m3", 3),
                ],
                columns=["date", "stock_id", "metric", "value"],
            ),
            id="Two stock id",
        ),
    ],
)
def test_process_stock_values(
    raw_stock_values: pd.DataFrame, expected_stock_values: pd.DataFrame
):
    stock_values = _process_stock_values(raw_stock_values.set_index("Date"))
    assert_frame_equal(stock_values, expected_stock_values)


@pytest.mark.parametrize(
    "day, expected_last_business_day",
    [
        pytest.param(dt.date(2021, 1, 4), dt.date(2021, 1, 4), id="Monday"),
        pytest.param(dt.date(2021, 1, 6), dt.date(2021, 1, 6), id="Wednesday"),
        pytest.param(dt.date(2021, 1, 8), dt.date(2021, 1, 8), id="Friday"),
        pytest.param(dt.date(2021, 1, 9), dt.date(2021, 1, 8), id="Saturday"),
        pytest.param(dt.date(2021, 1, 10), dt.date(2021, 1, 8), id="Sunday"),
    ],
)
def test_get_last_business_day(day, expected_last_business_day):
    last_business_day = get_last_business_day(day)
    assert last_business_day == expected_last_business_day
