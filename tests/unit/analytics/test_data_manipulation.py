import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture
from sqlalchemy.orm import Query

from pfa.analytics.data_manipulation import create_time_windows
from pfa.analytics.data_manipulation import fill_stock_data_to_time_horizon
from pfa.analytics.data_manipulation import loop_through_stocks


@pytest.mark.parametrize(
    "time_series_data",
    [
        pytest.param(
            pd.DataFrame({"date": pd.date_range("2020-01-01", "2020-06-01")}).assign(
                Value=1
            )
        ),
    ],
)
@pytest.mark.parametrize(
    "window_distance, window_size",
    [
        pytest.param(1, 30, id="Distance=1, Size=30"),
        pytest.param(10, 30, id="Distance=10, Size=30"),
        pytest.param(5, 33, id="Distance=5, Size=33"),
        pytest.param(7, 100, id="Distance=7, Size=100"),
        pytest.param(28, 28, id="Distance=28, Size=28"),
        pytest.param(30, 90, id="Distance=30, Size=90"),
    ],
)
def test_create_time_windows(
    time_series_data,
    window_distance,
    window_size,
):
    time_series_list = create_time_windows(
        time_series_data, window_distance, window_size
    )

    # Ensure list length is as expected.
    assert len(time_series_list) == np.ceil(
        (len(time_series_data) - window_size + 1) / window_distance
    )
    # Ensure all dataframes are the specified window size.
    assert np.array([len(df) == window_size for df in time_series_list]).all()
    # Ensure the last date is the last value in the frame
    assert (
        time_series_list[-1].iloc[-1, 0].date() == time_series_data.date.dt.date.max()
    )


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


@pytest.mark.parametrize(
    "stock_config, date_config",
    [pytest.param(pd.DataFrame([(1), (2), (3)], columns=["stock_id"]), pd.DataFrame())],
)
@pytest.mark.parametrize(
    "full_stock_data, expected_result",
    [
        pytest.param(
            pd.DataFrame([(1)], columns=["stock_id"]),
            pd.DataFrame([(1)]),
            id="One stock id in the full frame",
        ),
        pytest.param(
            pd.DataFrame([(1), (2)], columns=["stock_id"]),
            pd.DataFrame([(1), (1)]),
            id="One stock id in the full frame",
        ),
    ],
)
def test_loop_through_stocks(
    mocker: MockerFixture,
    stock_config: pd.DataFrame,
    date_config: pd.DataFrame,
    full_stock_data: pd.DataFrame,
    expected_result: pd.DataFrame,
):
    def example_func(param1, param2, param3):
        return pd.DataFrame([(1)])

    def mock_read_sql(query: Query):
        return {
            "stock_config": stock_config,
            "date_config": date_config,
        }[query._raw_columns[0].name]

    def mock_get_stock_data(stock_id):
        return full_stock_data.loc[full_stock_data["stock_id"] == stock_id, :]

    mocker.patch("pfa.analytics.data_manipulation.read_sql", mock_read_sql)
    mocker.patch("pfa.analytics.data_manipulation.get_stock_data", mock_get_stock_data)
    mocker.patch(
        "pfa.analytics.data_manipulation.fill_stock_data_to_time_horizon",
        lambda x, y: x,
    )

    result = loop_through_stocks(example_func)

    assert_frame_equal(result.reset_index(drop=True), expected_result)
