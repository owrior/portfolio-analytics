import pandas as pd
import prefect
import sqlalchemy as sqa
import yaml
from sqlalchemy_utils import create_database
from sqlalchemy_utils import database_exists
from sqlalchemy_utils import drop_database

from pfa.db import get_engine
from pfa.models.model import create_database_from_model
from pfa.readwrite import frame_to_sql


@prefect.task()
def initialise_database():
    engine = get_engine()

    if database_exists(engine.url):
        drop_database(engine.url)
    create_database(engine.url)

    create_database_from_model(engine)
    insert_ref_data()


def insert_ref_data():
    frame_to_sql(get_dates(), "date_config")

    with open("pfa/ref_data.yaml") as raw_ref_data:
        ref_data = yaml.safe_load(raw_ref_data)

    for table_name, contents in ref_data.items():
        frame_to_sql(pd.DataFrame(contents), table_name)


def get_dates(start: str = "1970-01-01", end: str = "2050-01-01") -> pd.DataFrame:
    return (
        pd.DataFrame({"date": pd.date_range(start, end)})
        .assign(date_id=lambda x: x.index)
        .loc[:, ["date_id", "date"]]
    )


def extract_columns(table: sqa.orm.DeclarativeMeta) -> list:
    return [*table._sa_class_manager.keys()]
