import numpy as np
import pandas as pd
import pytest

from pfa.analytics.data_manipulation import create_time_windows


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
