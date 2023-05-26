import plotly.express as px
import streamlit as st
from sqlalchemy.orm import Query
from utils.load_db_config import load_metric_config
from utils.load_db_config import load_stock_config

from pfa.models.config import DateConfig
from pfa.models.values import StockValues
from pfa.readwrite import read_sql

st.title("Historical Stock Performance")

col1, col2, col3 = st.columns([2, 1, 4])

STOCK_CONFIG = load_stock_config()


@st.cache_data
def load_historical_stock_data(
    stock,
):
    stock_config = load_stock_config()
    metric_config = load_metric_config()

    query = (
        Query(StockValues)
        .with_entities(
            DateConfig.date.label("date"),
            StockValues.stock_id.label("stock"),
            StockValues.metric_id.label("metric"),
            StockValues.value.label("value"),
        )
        .join(DateConfig, DateConfig.date_id == StockValues.date_id)
        .filter(
            StockValues.stock_id
            == int(
                stock_config.loc[
                    stock_config["yahoo_ticker"] == stock, "stock_id"
                ].iloc[0]
            ),
        )
    )

    data = read_sql(query).astype({"stock": "category", "metric": "category"})

    data["stock"] = data["stock"].cat.rename_categories(
        {
            x[0]: x[1]
            for x in zip(stock_config["stock_id"], stock_config["yahoo_ticker"])
        }
    )
    data["metric"] = data["metric"].cat.rename_categories(
        {x[0]: x[1] for x in zip(metric_config["metric_id"], metric_config["metric"])}
    )
    return data, data["metric"].unique().to_list()


with col1:
    stock_option = st.selectbox("Stock", load_stock_config()["yahoo_ticker"].to_list())
    data, metric_options = load_historical_stock_data(stock_option)
    metric_option = st.selectbox("Metric", metric_options)
    rolling_option = st.slider("Rolling Weeks", 0, 8, 0)

if rolling_option > 0:
    data["value"] = data.groupby("metric")["value"].transform(
        lambda x: x.rolling(rolling_option * 7).mean()
    )
    data = data.dropna()

stock_name = STOCK_CONFIG.loc[
    STOCK_CONFIG["yahoo_ticker"] == stock_option, "stock"
].iloc[0]
fig = px.line(
    data.loc[data["metric"] == metric_option],
    title=f"{stock_name}",
    x="date",
    y="value",
    labels={"date": "Date", "value": metric_option},
).update_layout(height=500)

with col3:
    st.plotly_chart(fig, use_container_width=True)
