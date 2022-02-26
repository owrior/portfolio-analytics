from dash import html
import dash_bootstrap_components as dbc

layout = html.Div(
    children=[dbc.Row(children=[dbc.Col("Col 1"), dbc.Col("Col 2")])],
    className="container",
)
