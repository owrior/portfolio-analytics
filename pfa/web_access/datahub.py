"""
Functionality for downloading all parameters specified to come from datahub.io
"""
import datetime as dt

import numpy as np
import pandas as pd
from datapackage import Package
from sqlalchemy.orm import Query

from pfa.models.config import DateConfig
from pfa.readwrite import frame_to_sql
from pfa.readwrite import read_sql
from pfa.web_access.update_and_cache import get_most_recent_datahub_parameter_dates


def populate_datahub_parameter_values():

    date_config = read_sql(Query(DateConfig))
    parameter_config = get_most_recent_datahub_parameter_dates()

    if len(parameter_config) == 0:
        return

    parameter_values = _download_and_process_parameter_values(
        parameter_config, date_config
    )
    if parameter_values is not None:
        frame_to_sql(parameter_values, "parameter_values")


def _download_and_process_parameter_values(
    parameter_config: pd.DataFrame, date_config: pd.DataFrame
) -> pd.DataFrame:
    parameter_values = []
    for _, row in parameter_config.iterrows():
        start_date = (
            pd.Timestamp(1900, 1, 1)
            if row.date in (pd.NaT, None)
            else pd.Timestamp(row.date + dt.timedelta(days=1))
        )

        package = Package(row["url"])
        res = pd.DataFrame(
            package.get_resource(row["resource_name"]).read(), columns=["date", "value"]
        ).astype({"date": "datetime64", "value": "float"})

        parameter_values.append(
            res.loc[res["date"] > start_date]
            .merge(date_config, on="date", how="inner")
            .assign(
                parameter_id=row.parameter_id,
                value=lambda x: np.round(x.value, decimals=4),
            )
            .loc[:, ["parameter_id", "date_id", "value"]]
        )
    return pd.concat(parameter_values) if len(parameter_values) > 0 else None
