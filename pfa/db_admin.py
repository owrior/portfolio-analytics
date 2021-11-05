import pandas as pd
from pathlib import Path
from sqlalchemy_utils import database_exists
from sqlalchemy_utils import create_database
from sqlalchemy_utils import drop_database

from pfa.models.base import Base

from pfa.db import get_engine
from pfa.readwrite import frame_to_sql


def initialise_database():
    engine = get_engine()

    if database_exists(engine.url):
        drop_database(engine.url)
    create_database(engine.url)

    Base.metadata.create_all(engine)

    insert_ref_data()


def insert_ref_data():
    date = _get_dates()
    frame_to_sql(date, "date")

    ref_data_folder = Path(__file__).parents[1] / "ref_data"

    ref_data_files = ref_data_folder.glob("*.csv")

    for file in ref_data_files:
        table_name = file.name.split(".")[0]
        ref_data = pd.read_csv(ref_data_folder / file, sep=";")
        frame_to_sql(ref_data, table_name)


def _get_dates(start: str = "1970-01-01", end: str = "2050-01-01") -> pd.DataFrame:
    return pd.DataFrame({"date": pd.date_range(start, end)}).assign(
        date_id=lambda x: x.index
    )
