from datetime import date
from datetime import timedelta

import streamlit as st
from sqlalchemy.orm import Query
from utils.load_db_config import extract_id
from utils.load_db_config import load_metric_config

from pfa.models.config import DateConfig
from pfa.models.values import StockValues
from pfa.readwrite import read_sql

st.set_page_config(layout="wide")

st.title("Home")

col1, col2, col3 = st.columns([4, 1, 4])

METRIC_CONFIG = load_metric_config()


@st.cache_data
def load_historical_change():
    date_ranges = {
        "90 Day Change": [
            date.today() - timedelta(days=91),
            date.today() - timedelta(days=181),
        ],
        "30 Day Change": [
            date.today() - timedelta(days=31),
            date.today() - timedelta(days=61),
        ],
    }

    query = (
        Query(StockValues)
        .with_entities(
            DateConfig.date.label("date"),
            StockValues.stock_id.label("stock"),
            StockValues.metric_id.label("metric"),
            StockValues.value.label("value"),
        )
        .join(DateConfig, DateConfig.date_id == StockValues.date_id)
        .filter(DateConfig.date > date.today() - timedelta(days=181))
    )
    data = read_sql(query)

    for key, val in date_ranges.items():
        adj_close_id = extract_id("Adj Close", METRIC_CONFIG)
        data.loc[
            (data["date"].between_time(date.today(), val[0], inclusive="left"))
            & (data["metric"] == adj_close_id)
        ]
        data.loc[
            (data["date"].between_time(val[0], val[1], inclusive="left"))
            & (data["metric"] == adj_close_id)
        ]


with col1:
    pass
    # load_historical_change()

with col3:
    pass
