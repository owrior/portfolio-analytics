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
