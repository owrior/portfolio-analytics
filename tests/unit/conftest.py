import pytest
import pandas as pd
from typing import List

from tests.utilities.dataframe import read_string_as_csv


def gen_raw_stock_values(tickers: List[str]) -> pd.DataFrame:
    return (
        pd.concat(
            [
                pd.DataFrame(
                    {"Date": pd.date_range("01-01-2020", end="05-01-2020")}
                ).assign(level_1=ticker)
                for ticker in tickers
            ]
        )
        .merge(
            pd.DataFrame(
                {
                    "Adj Close": [100],
                    "Close": [100],
                    "High": [100],
                    "Low": [100],
                    "Open": [100],
                    "Volume": [100],
                }
            ),
            how="cross",
        )
        .set_index(["Date", "level_1"])
        .unstack()
    )


def gen_processed_stock_values(tickers: List[str]) -> pd.DataFrame:
    columns = ["metric", "yahoo_ticker", "date", "value"]
    return (
        pd.DataFrame(
            {
                "metric": ["Adj Close", "Close", "High", "Low", "Open", "Volume"],
                "value": [100, 100, 100, 100, 100, 100],
            }
        )
        .merge(
            pd.DataFrame({"date": pd.date_range("01-01-2020", end="05-01-2020")}),
            how="cross",
        )
        .merge(pd.DataFrame({"yahoo_ticker": tickers}), how="cross")
        .loc[:, columns]
        .sort_values(columns)
        .reset_index(drop=True)
    )


@pytest.fixture
def date_config():
    return read_string_as_csv(
        """
        date_id;date
        0;2020-01-01
        1;2020-01-02
        2;2020-01-03
        3;2020-01-04
        4;2020-01-05
        5;2020-01-06
        6;2020-01-07
        7;2020-01-08
        8;2020-01-09
        9;2020-01-10
        """
    ).astype({"date": "datetime64[s]"})
