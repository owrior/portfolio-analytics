from dash import dcc, html, dash
from pfa.db import get_engine
from sqlalchemy import Table, MetaData
import pandas as pd

app = dash.Dash(__name__)

data = pd.read_sql("SELECT * FROM forecasts", get_engine())

data["date"] = pd.to_datetime(data["date"])

data = data.loc[
    (data["stock"] == "GOOG")
    & (data["analysis"] == "XGBoost Regression")
    & (data["metric"] == "Adj Close"),
    :,
].sort_values("date")

app.layout = app.layout = html.Div(
    children=[
        html.H1(
            children="Avocado Analytics",
        ),
        html.P(
            children="Analyze the behavior of avocado prices"
            " and the number of avocados sold in the US"
            " between 2015 and 2018",
        ),
        dcc.Graph(
            figure={
                "data": [
                    {
                        "x": data["date"],
                        "y": data["value"],
                        "type": "lines",
                    },
                ],
                "layout": {"title": "Average Price of Avocados"},
            },
        ),
    ]
)
