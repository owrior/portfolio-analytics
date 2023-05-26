import streamlit as st
from sqlalchemy.orm import Query

from pfa.models.config import MetricConfig
from pfa.models.config import StockConfig
from pfa.readwrite import read_sql


@st.cache_data
def load_stock_config():
    return read_sql(Query(StockConfig))


@st.cache_data
def load_metric_config():
    return read_sql(Query(MetricConfig))


def extract_id(value, table):
    cols = table.columns
    return table.loc[table[cols[1]] == value, cols[0]].iloc[0]
