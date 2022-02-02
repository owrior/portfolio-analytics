import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture

from pfa.analytics.prophet import fill_stock_data_to_time_horizon
from pfa.analytics.prophet import generate_validation_metrics


class MockMetricIDCache:
    @property
    def mean_error(self):
        return 1

    @property
    def mean_abs_error(self):
        return 2

    @property
    def rmse(self):
        return 3


@pytest.mark.parametrize(
    "true_data, predicted_data, expected_result",
    [
        pytest.param(
            pd.DataFrame([(1, 1)], columns=["ds", "y"]),
            pd.DataFrame([(1, 1)], columns=["ds", "yhat"]),
            pd.DataFrame([(1, 0.0, 0.0, 0.0)], columns=["date", 1, 2, 3]),
        )
    ],
)
def test_generate_validation_metrics(
    mocker: MockerFixture,
    true_data: pd.DataFrame,
    predicted_data: pd.DataFrame,
    expected_result: pd.DataFrame,
):
    mocker.patch("pfa.analytics.prophet.metric_id_cache", MockMetricIDCache())
    result = generate_validation_metrics(true_data, predicted_data)

    assert_frame_equal(result, expected_result)


@pytest.mark.parametrize(
    "date_config", [pytest.param(pd.DataFrame({"date": np.arange(0, 100)}))]
)
@pytest.mark.parametrize(
    "stock_data, value_sum",
    [
        pytest.param(
            pd.DataFrame([(10, 4), (20, 19)], columns=["ds", "value"]),
            (20 - 10) * 4 + 19,
            id="Check filling and bounding works",
        ),
        pytest.param(
            pd.DataFrame([(10, 4), (20, 19), (99, 0)], columns=["ds", "value"]),
            (20 - 10) * 4 + (99 - 20) * 19,
            id="Check filled to full horizon when value inplace",
        ),
        pytest.param(
            pd.DataFrame([(20, 19), (10, 4)], columns=["ds", "value"]),
            (20 - 10) * 4 + 19,
            id="Check order is unimportant",
        ),
    ],
)
def test_fill_stock_data_to_time_horizon(
    mocker: MockerFixture, stock_data, date_config, value_sum
):
    mocker.patch("pfa.analytics.prophet.metric_id_cache")
    filled_stock_data = fill_stock_data_to_time_horizon(stock_data, date_config)
    assert filled_stock_data["value"].sum() == value_sum
