import pytest
from pytest_mock import MockerFixture
from pandas.testing import assert_frame_equal
from typing import List

from tests.unit.conftest import gen_raw_stock_values
from tests.unit.conftest import gen_processed_stock_values
from tests.utilities.db_mocks import mock_config_data

from pfa.web_access.yahoo_finance import _process_stock_values
from pfa.web_access.yahoo_finance import populate_yahoo_stock_values


@pytest.mark.parametrize(
    "tickers",
    [["STOCK_0"], ["STOCK_0", "STOCK_1"], ["STOCK_" + str(i) for i in range(10)]],
)
def test_process_stock_values(tickers: List[str]):
    raw_stock_values = gen_raw_stock_values(tickers)
    expected_stock_values = gen_processed_stock_values(tickers)

    stock_values = _process_stock_values(raw_stock_values)

    assert_frame_equal(
        stock_values.sort_values(["metric", "yahoo_ticker", "date", "value"]),
        expected_stock_values,
    )


def test_populate_yahoo_stock_values(mocker: MockerFixture):
    config_data = mock_config_data(["stock_config", "date_config", "metric_config"])
    mocker.patch(
        "pfa.web_access.yahoo_finance._get_config_tables",
        return_value=config_data,
    )
    mocker.patch(
        "pfa.web_access.yahoo_finance._download_stock_values",
        side_effect=gen_raw_stock_values,
    )
    mocker.patch("pfa.web_access.yahoo_finance.frame_to_sql")

    tickers = mock_config_data(["stock_config"])[0]["yahoo_ticker"].to_list()
    stock_config, date_config, metric_config = config_data
    expected_stock_values = (
        gen_processed_stock_values(tickers)
        .merge(date_config, on="date", how="inner")
        .merge(metric_config, on="metric", how="inner")
        .merge(stock_config, on="yahoo_ticker", how="inner")
        .loc[:, ["stock_id", "date_id", "metric_id", "value"]]
    )

    stock_values = populate_yahoo_stock_values()

    assert_frame_equal(stock_values, expected_stock_values)
