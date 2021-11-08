import pandas as pd
from pandas.testing import assert_frame_equal

from pfa.db_admin import _get_dates


def test_get_dates(date_config: pd.DataFrame):
    result = _get_dates("01-01-2020", "01-10-2020")
    assert_frame_equal(result, date_config)
