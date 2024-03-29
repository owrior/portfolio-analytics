import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dash, dcc, html

from pfa.dash.pages import forecasting_tools, index

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])
server = app.server

base_content = html.Div(
    children=[
        html.Div(
            children=[
                dcc.Location(id="url", refresh=False),
                dbc.Navbar(
                    children=[
                        dbc.NavItem(dbc.NavLink("Home", href="./")),
                        dbc.NavItem(
                            dbc.NavLink("Forecasting tools", href="/forecasting_tools")
                        ),
                    ]
                ),
            ]
        ),
        html.Div(id="page-content"),
    ]
)

app.layout = base_content


@callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    if pathname == "/forecasting_tools":
        return forecasting_tools.layout
    else:
        return index.layout
