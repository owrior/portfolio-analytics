import datetime as dt

import numpy as np
import pandas as pd
import pytest
from pytest_mock import MockerFixture

from pfa.analytics.data_manipulation import create_time_windows
from pfa.analytics.data_manipulation import fill_stock_data_to_time_horizon
from pfa.analytics.data_manipulation import get_cutoffs


@pytest.mark.parametrize(
    "time_series_data",
    [
        pytest.param(
            pd.DataFrame({"date": pd.date_range("2020-01-01", periods=360)}).assign(
                Value=1
            )
        ),
        pytest.param(
            pd.DataFrame({"date": pd.date_range("2020-01-01", periods=360)})
            .assign(Value=1)
            .iloc[1:, :]
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
    "dates",
    [
        pytest.param(pd.Series(pd.date_range("2020-01-01", periods=365))),
    ],
)
@pytest.mark.parametrize("initial, horizon", [pytest.param(180, 30)])
def test_get_cutoffs(dates, initial, horizon):
    cutoffs = get_cutoffs(dates, initial, horizon)
    cutoff_frame = pd.DataFrame({"cutoff": cutoffs})

    assert (cutoff_frame["cutoff"].diff().dropna() == dt.timedelta(days=30)).all()
    assert (dates.iloc[-1] - cutoff_frame["cutoff"] >= dt.timedelta(days=30)).all()


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
