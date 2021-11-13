"""
Functionality for downloading all parameters specified to come from datahub.io
"""
from typing import Tuple

import numpy as np
import pandas as pd
from datapackage import Package
from sqlalchemy.orm import Query

from pfa.models.date_config import DateConfig
from pfa.models.parameter_config import ParameterConfig
from pfa.readwrite import frame_to_sql, read_sql


def populate_datahub_parameter_values():

    parameter_config, date_config = _get_config_tables()

    if len(parameter_config) == 0:
        return

    parameter_values = _download_and_process_parameter_values(
        parameter_config, date_config
    )
    frame_to_sql(parameter_values, "parameter_values")


def _get_config_tables() -> Tuple[pd.DataFrame, pd.DataFrame]:  # pragma: no cover
    parameter_config = read_sql(
        Query(ParameterConfig)
        .with_entities(
            ParameterConfig.parameter_id,
            ParameterConfig.url,
            ParameterConfig.resource_name,
        )
        .filter(ParameterConfig.url.like("%datahub.io%"))
    )
    date_config = read_sql(Query(DateConfig))
    return parameter_config, date_config


def _download_and_process_parameter_values(
    parameter_config: pd.DataFrame, date_config: pd.DataFrame
) -> pd.DataFrame:
    parameter_values = []
    for index, row in parameter_config.iterrows():
        package = Package(row["url"])
        res = package.get_resource(row["resource_name"]).read()

        parameter_values.append(
            pd.DataFrame(res, columns=["date", "value"])
            .astype({"date": "datetime64", "value": "float"})
            .merge(date_config, on="date", how="inner")
            .assign(
                parameter_id=row["parameter_id"],
                value=lambda x: np.round(x.value, decimals=4),
            )
            .loc[:, ["parameter_id", "date_id", "value"]]
        )
    return pd.concat(parameter_values)
