import plotly.express as px
import streamlit as st
from utils.dropdown_data import get_dropdown_options

from pfa.readwrite import read_view

st.set_page_config(layout="wide")

st.title("Home")

col1, col2, col3 = st.columns([2, 1, 4])

with col1:
    stock_option = st.selectbox(
        "Stock", get_dropdown_options("stock_config", "stock", "historical")
    )
    metric_option = st.selectbox(
        "Metric", get_dropdown_options("metric_config", "metric", "historical")
    )
    rolling_option = st.slider("Rolling Weeks", 0, 8, 0)

data = (
    read_view(
        "historical", where=f"stock='{stock_option}' AND metric='{metric_option}' "
    )
    .rename(columns={"date": "Date", "value": f"{metric_option}"})
    .loc[:, ["Date", metric_option]]
)

if rolling_option > 0:
    data[metric_option] = data[metric_option].rolling(rolling_option * 7).mean()
    data = data.dropna()

fig = px.line(
    data,
    title=f"{stock_option}",
    x="Date",
    y=f"{metric_option}",
).update_layout(height=500)

with col3:
    st.plotly_chart(fig, use_container_width=True)
