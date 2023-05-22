"""
Functionality for downloading all parameters specified to come from datahub.io
"""
import datetime as dt

import numpy as np
import pandas as pd
import prefect
from datapackage import Package
from sqlalchemy.orm import Query

from pfa.models.config import DateConfig
from pfa.readwrite import frame_to_sql, read_sql


@prefect.task
def populate_datahub_parameter_values(parameter_dates):
    date_config = read_sql(Query(DateConfig))

    parameter_values = _download_and_process_parameter_values(
        parameter_dates, date_config
    )
    if parameter_values is not None:
        frame_to_sql(parameter_values, "parameter_values")


def _download_and_process_parameter_values(
    parameter_dates: pd.DataFrame, date_config: pd.DataFrame
) -> pd.DataFrame:
    start_date = (
        pd.Timestamp(1900, 1, 1)
        if parameter_dates.date in (pd.NaT, None)
        else pd.Timestamp(parameter_dates.date + dt.timedelta(days=1))
    )

    package = Package(parameter_dates.url)
    res = pd.DataFrame(
        package.get_resource(parameter_dates.resource_name).read(),
        columns=["date", "value"],
    ).astype({"date": "datetime64", "value": "float"})

    parameter_values = (
        res.loc[res["date"] > start_date]
        .merge(date_config, on="date", how="inner")
        .assign(
            parameter_id=parameter_dates.parameter_id,
            value=lambda x: np.round(x.value, decimals=4),
        )
        .loc[:, ["parameter_id", "date_id", "value"]]
    )
    return parameter_values
