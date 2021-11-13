import datetime
from decimal import Decimal

import pandas as pd
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture

from pfa.web_access.datahub import _download_and_process_parameter_values
from tests.utilities.db_mocks import mock_config_data


class MockPackage:
    def __init__(self, url) -> None:
        pass

    def get_resource(self, name):
        return self

    def read(self):
        return [
            [datetime.date(2020, 1, 1), Decimal(1.00001)],
            [datetime.date(2020, 1, 2), Decimal(1.00001)],
            [datetime.date(2020, 1, 3), Decimal(1.00001)],
            [datetime.date(2020, 1, 4), Decimal(1.00001)],
            [datetime.date(2020, 1, 5), Decimal(1.00001)],
        ]


def test_download_and_process_parameter_values(mocker: MockerFixture):
    mocker.patch("pfa.web_access.datahub.Package", side_effect=MockPackage)

    parameter_config, date_config = mock_config_data(
        ["parameter_config", "date_config"]
    )
    res = _download_and_process_parameter_values(parameter_config, date_config)

    assert_frame_equal(
        res.reset_index(drop=True),
        pd.DataFrame(
            {
                "parameter_id": {
                    0: 1,
                    1: 1,
                    2: 1,
                    3: 1,
                    4: 1,
                    5: 2,
                    6: 2,
                    7: 2,
                    8: 2,
                    9: 2,
                },
                "date_id": {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 0, 6: 1, 7: 2, 8: 3, 9: 4},
                "value": {
                    0: 1.0,
                    1: 1.0,
                    2: 1.0,
                    3: 1.0,
                    4: 1.0,
                    5: 1.0,
                    6: 1.0,
                    7: 1.0,
                    8: 1.0,
                    9: 1.0,
                },
            }
        ),
    )
