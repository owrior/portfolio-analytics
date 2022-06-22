import pandas as pd
import sqlalchemy as sqa
from sqlalchemy_utils import create_database
from sqlalchemy_utils import database_exists
from sqlalchemy_utils import drop_database

from pfa.db import get_engine
from pfa.models.model import create_database_from_model
from pfa.models.model import execute_view_creation
from pfa.readwrite import frame_to_sql


def initialise_database():
    engine = get_engine()

    if database_exists(engine.url):
        drop_database(engine.url)
    create_database(engine.url)

    create_database_from_model(engine)
    insert_ref_data()
    execute_view_creation()


def insert_ref_data():
    date_config = _get_dates()
    frame_to_sql(date_config, "date_config")

    ref_data_folder = "ref_data"

    ref_data_files = ref_data_folder.glob("*.csv")

    for file in ref_data_files:
        table_name = file.name.split(".")[0]
        ref_data = pd.read_csv(ref_data_folder / file, sep=";")
        frame_to_sql(ref_data, table_name)


def _get_dates(start: str = "1970-01-01", end: str = "2050-01-01") -> pd.DataFrame:
    return (
        pd.DataFrame({"date": pd.date_range(start, end)})
        .assign(date_id=lambda x: x.index)
        .loc[:, ["date_id", "date"]]
    )


def extract_columns(table: sqa.orm.DeclarativeMeta) -> list:
    return [*table._sa_class_manager.keys()]
