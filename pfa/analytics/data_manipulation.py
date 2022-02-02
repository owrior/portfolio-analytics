from typing import List

import numpy as np
import pandas as pd


def create_time_windows(
    time_series_data: pd.DataFrame, window_distance: int, window_size: int
) -> List[pd.DataFrame]:
    """
    Parameters
    ----------
    time_series_data : pd.DataFrame
        Data in the form of a date column (named date).
    window_start_delay : int
        The number of days between the beginning of each window.
    window_size : int
        Size of time windows in days.
    """
    index_values = time_series_data.index.values
    index_list = [
        index_values[i : i + window_size]
        for i in np.arange(
            0,
            index_values.shape[0] - window_size + 1,
            step=window_distance,
        )
        + np.remainder(index_values.shape[0] - window_size, window_distance)
    ]
    return [time_series_data.iloc[index, :] for index in index_list]
