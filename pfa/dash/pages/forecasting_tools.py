from dash import html
from dash import dcc
from dash import callback
from dash import Input
from dash import Output
import dash_bootstrap_components as dbc
import plotly.express as px

from pfa.readwrite import read_view

validation_metrics = read_view("validation_metrics")

layout = html.Div(
    children=[
        dbc.Row(
            children=[dcc.Graph(figure=px.line(validation_metrics)), dbc.Col("Col 2")]
        )
    ],
    className="container",
)
