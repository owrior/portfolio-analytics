from typing import List

import pandas as pd


def mock_config_data(tables: List[str]) -> List[pd.DataFrame]:
    config_data = {
        "currency_config": pd.DataFrame(
            {"currency_id": [1, 2], "currency": ["GBP", "USD"], "symbol": ["£", "$"]}
        ),
        "metric_config": pd.DataFrame(
            {
                "metric_id": [1, 2, 3, 4, 5, 6],
                "metric": ["Adj Close", "Close", "High", "Low", "Open", "Volume"],
            }
        ),
        "parameter_config": pd.DataFrame(
            {
                "parameter_id": [1, 2],
                "currency_id": [1, 1],
                "parameter": ["A", "B"],
                "url": ["A", "B"],
                "resource_name": ["A", "B"],
            }
        ),
        "portfolio_config": pd.DataFrame(
            {
                "portfolio_id": range(2),
                "portfolio": ["testing_portfolio_" + str(i) for i in range(2)],
            }
        ),
        "stock_config": pd.DataFrame(
            {
                "stock_id": range(3),
                "currency_id": [1 for i in range(3)],
                "stock": ["STOCK_" + str(i) for i in range(3)],
                "yahoo_ticker": ["STOCK_" + str(i) for i in range(3)],
            }
        ),
        "date_config": pd.DataFrame(
            {"date": pd.date_range("01-01-2020", "05-01-2020")}
        ).assign(date_id=lambda x: x.index),
    }
    return [config_data[table] for table in tables]
