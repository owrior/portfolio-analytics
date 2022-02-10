import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal

from pfa.analytics.sklearn import get_xy


@pytest.mark.parametrize(
    "df, window_size, expected_x, expected_y",
    [
        pytest.param(
            pd.DataFrame(
                [(1, 1) for i in np.arange(30)],
                columns=["x", "y"],
            ),
            30,
            np.ones((30, 1)),
            np.ones((30,)),
            id="Single x column",
        ),
        pytest.param(
            pd.DataFrame(
                [(1, 1, 1) for i in np.arange(30)],
                columns=["x", "x_bar_7", "y"],
            ),
            30,
            np.ones((30, 2)),
            np.ones((30,)),
            id="Two x columns",
        ),
        pytest.param(
            pd.DataFrame(
                [(1, 1, 1, 1) for i in np.arange(30)],
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
