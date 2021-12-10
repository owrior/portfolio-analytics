from typing import List

import pytest
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture

from pfa.web_access.yahoo_finance import (
    _process_stock_values,
    populate_yahoo_stock_values,
)
from tests.unit.conftest import gen_processed_stock_values, gen_raw_stock_values
from tests.utilities.db_mocks import mock_config_data
