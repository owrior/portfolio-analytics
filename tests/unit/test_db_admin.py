import pandas as pd
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture

from pfa.db_admin import get_dates
from pfa.db_admin import insert_ref_data


def test_get_dates(date_config: pd.DataFrame):
    result = get_dates("01-01-2020", "01-10-2020")
    assert_frame_equal(result, date_config)


def test_insert_ref_data(mocker: MockerFixture):
    mocked_frame_to_sql = mocker.patch("pfa.db_admin.frame_to_sql")
    insert_ref_data()

    mocked_frame_to_sql.assert_called()
