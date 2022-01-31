import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture

from pfa.readwrite import frame_to_sql


@pytest.mark.parametrize(
    "df",
    [
        pytest.param(pd.DataFrame([], columns=[1]), id="Empty table"),
        pytest.param(pd.DataFrame([(1)], columns=[1]), id="Populated table"),
    ],
)
def test_frame_to_sql(mocker: MockerFixture, df: pd.DataFrame):
    mock_to_sql = mocker.patch("pfa.readwrite.pd.DataFrame.to_sql")

    frame_to_sql(df, "Test")
    if df.empty:
        mock_to_sql.assert_not_called()
    else:
        mock_to_sql.assert_called_once()
