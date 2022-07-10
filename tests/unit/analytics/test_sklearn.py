import logging

import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_almost_equal
from numpy.testing import assert_array_equal
from sklearn.linear_model import LinearRegression

from pfa.analytics.sklearn import create_features
from pfa.analytics.sklearn import get_xy
from pfa.analytics.sklearn import predict_forward
from pfa.analytics.sklearn import transform_prediction_and_create_x
from pfa.db_admin import _get_dates

logger = logging.getLogger()


@pytest.mark.parametrize(
    "df, window_size, expected_x, expected_y",
    [
        pytest.param(
            pd.DataFrame([(1, 1) for _ in np.arange(30)], columns=["x", "y"]),
            30,
            np.ones((30, 1)),
            np.ones((30,)),
            id="Single x column",
        ),
        pytest.param(
            pd.DataFrame(
                [(1, 1, 1) for _ in np.arange(30)], columns=["x", "x_bar_7", "y"]
            ),
            30,
            np.ones((30, 2)),
            np.ones((30,)),
            id="Two x columns",
        ),
        pytest.param(
            pd.DataFrame(
                [(1, 1, 1, 1) for _ in np.arange(30)],
                columns=["x", "x_bar_7", "x_bar_30", "y"],
            ),
            30,
            np.ones((30, 3)),
            np.ones((30,)),
            id="Two x columns",
        ),
    ],
)
def test_get_xy(
    df: pd.DataFrame, window_size: int, expected_x: np.array, expected_y: np.array
) -> None:
    x, y = get_xy(df, window_size)

    assert_array_equal(x, expected_x)
    assert_array_equal(y, expected_y)


@pytest.mark.parametrize(
    "stock_data, date_config, training_period, forecast_length, expected_result",
    [
        pytest.param(
            pd.DataFrame(
                {
                    "ds": pd.date_range("2020-01-01", periods=31),
                    "y": np.arange(31) + 1,
                    "adj_close": np.arange(31) + 100,
                }
            ),
            _get_dates(start="2020-01-01", end="2050-01-01"),
            30,
            5,
            np.array([133.63, 136.71, 139.25, 141.30, 142.92]),
        )
    ],
)
def test_predict_forward(
    stock_data, date_config, training_period, forecast_length, expected_result
):
    m = LinearRegression
    stock_data = (
        stock_data.pipe(transform_prediction_and_create_x)
        .pipe(create_features)
        .reset_index(drop=True)
    )
    result = predict_forward(
        m,
        stock_data,
        date_config,
        training_period,
        forecast_length,
        kwargs={},
    )
    assert_almost_equal(
        result.tail(forecast_length)["adj_close"].values.round(2), expected_result
    )
