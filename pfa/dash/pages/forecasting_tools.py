from dash import html
from dash import dcc
from dash import callback
from dash import Input
from dash import Output
import dash_bootstrap_components as dbc
import plotly.express as px
import sqlalchemy as sqa

from pfa.readwrite import read_view


def except_missing_db(element_fn):
    try:
        return element_fn()
    except sqa.exc.OperationalError:
        return "Database error"


def validation_figure():
    return dcc.Graph(
        figure=px.line(
            read_view(
                "validation_metrics",
                where='stock="Polarian Imaging Ltd" AND metric="RMSE"',
            ),
            x="date",
            y="value",
            color="analysis",
        )
    )


layout = html.Div(
    children=[
        dbc.Row(
            children=[
                dbc.Col(except_missing_db(validation_figure)),
                dbc.Col("Col 2"),
            ]
        )
    ],
    className="container",
)
