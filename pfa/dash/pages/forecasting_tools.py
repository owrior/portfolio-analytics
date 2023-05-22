import dash_bootstrap_components as dbc
import plotly.express as px
from dash import Input, Output, callback, dcc, html

from pfa.dash.figure import except_missing_db, get_date_kwargs, get_dropdown_options
from pfa.readwrite import read_view

stock_dropdown = get_dropdown_options("stock_config", "stock", "forecasts")

metric_dropdown = get_dropdown_options("metric_config", "metric", "forecasts")

layout = html.Div(
    children=[
        dbc.Row(
            children=[
                dcc.Dropdown(
                    id="stock-dropdown",
                    options=stock_dropdown,
                    value=stock_dropdown[0],
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
                    ),
                    width=10,
                ),
                dbc.Col(
                    children=[
                        dcc.Dropdown(
                            id="metric-dropdown",
                            options=metric_dropdown,
                            value=metric_dropdown[0],
                        ),
                        dcc.DatePickerRange(
                            id="date-slider",
                            **get_date_kwargs("forecasts"),
                        ),
                    ],
                    width=2,
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
            read_view("validation_metrics", where=f"stock='{stock}'",).rename(
                columns={
                    "date": "Date",
                    "value": "Metric",
                    "analysis": "Analysis",
                }
            ),
            title=f"Model validation - {stock}",
            x="Date",
            y="Metric",
            color="Analysis",
            facet_row="metric",
        )
        # Removes equivalent axes.
        .update_yaxes(matches=None)
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
                where=f"stock='{stock}' AND metric='{metric}' "
                f"AND date BETWEEN DATE('{start_date}') AND DATE('{end_date}')",
            ).rename(
                columns={"date": "Date", "value": f"{metric}", "analysis": "Analysis"}
            ),
            title=f"Forecast - {stock}",
            x="Date",
            y=f"{metric}",
            color="Analysis",
        )
    )
