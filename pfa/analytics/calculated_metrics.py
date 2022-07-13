import numpy as np

from pfa.id_cache import metric_id_cache


def rmse(y_true, y_pred):
    """
    Calculates the root mean squared error.
    """
    return np.sqrt(np.mean(np.square(y_true - y_pred)))


def rmsle(y_true, y_pred):
    """
    Calculates the root mean squared log errors.
    """
    return rmse(np.log(y_true + 1), np.log(y_pred + 1))


def smape(y_true, y_pred):
    return np.mean(
        np.absolute(y_pred - y_true) / ((np.absolute(y_true) + np.absolute(y_pred)) / 2)
    )


def get_metric_function_mapping():
    """
    Maps metric ids to functions.
    """
    return {
        metric_id_cache.rmse: rmse,
        metric_id_cache.rmsle: rmsle,
        metric_id_cache.smape: smape,
    }.items()
