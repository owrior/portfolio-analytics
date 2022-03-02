from dash import html
from dash import dcc
from dash import callback
from dash import Input
from dash import Output
import dash_bootstrap_components as dbc
import plotly.express as px
import datetime as dt

from pfa.readwrite import read_view
from pfa.dash.figure import except_missing_db
from pfa.dash.figure import get_dropdown_options
from pfa.dash.figure import get_date_range


stock_dropdown = except_missing_db(
    get_dropdown_options("validation_metrics", "stock"), exception_return=["Error"]
)
metric_dropdown = except_missing_db(
    get_dropdown_options("forecasts", "metric"), exception_return=["Error"]
)
forecast_date_range = get_date_range("forecasts")


layout = html.Div(
    children=[
        dbc.Row(
            children=[
                dbc.Col(width=9),
                dbc.Col(
                    children=[
                        dcc.Dropdown(
                            id="metric-dropdown",
                            options=metric_dropdown,
                            value=metric_dropdown[0],
                        ),
                        dcc.Dropdown(
                            id="stock-dropdown",
                            options=stock_dropdown,
                            value=stock_dropdown[0],
                        ),
                        dcc.DatePickerRange(
                            id="date-slider",
                            start_date=dt.date.today(),
                            end_date=forecast_date_range["max"],
                            min_date_allowed=forecast_date_range["min"],
                            max_date_allowed=forecast_date_range["max"],
                        ),
                    ],
                    width=3,
                ),
            ],
            style={"padding-top": "1%"},
        ),
        dbc.Row(
            children=[
                dcc.Graph(
                    id="validation-figure",
                )
            ]
        ),
        dbc.Row(
            children=[
                dbc.Col(
                    dcc.Graph(
                        id="forecast-figure",
                    )
                ),
            ]
        ),
    ],
    className="container",
)


@callback(Output("validation-figure", "figure"), Input("stock-dropdown", "value"))
def update_validation_figure(stock):
    return except_missing_db(
        px.line(
            read_view(
                "validation_metrics",
                where=f'stock="{stock}"',
            ).rename(columns={"date": "Date", "value": "RMSE", "analysis": "Analysis"}),
            title=f"Model validation - {stock}",
            x="Date",
            y="RMSE",
            color="Analysis",
            facet_col="metric",
        )
    )


@callback(
    Output("forecast-figure", "figure"),
    [
        Input("stock-dropdown", "value"),
        Input("metric-dropdown", "value"),
        Input("date-slider", "start_date"),
        Input("date-slider", "end_date"),
    ],
)
def update_forecast_figure(stock, metric, start_date, end_date):
    return except_missing_db(
        px.line(
            read_view(
                "forecasts",
                where=f'stock="{stock}" AND metric="{metric}" AND date BETWEEN DATE("{start_date}") AND DATE("{end_date}")',
            ).rename(
                columns={"date": "Date", "value": f"{metric}", "analysis": "Analysis"}
            ),
            title=f"Forecast - {stock}",
            x="Date",
            y=f"{metric}",
            color="Analysis",
        )
    )
