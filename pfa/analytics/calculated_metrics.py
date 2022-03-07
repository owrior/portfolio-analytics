import numpy as np


def rmse(y_true, y_pred):
    return np.sqrt(np.mean(np.square(y_true - y_pred)))


def rmsle(y_true, y_pred):
    return rmse(np.log(y_true + 1), np.log(y_pred + 1))


def smape(y_true, y_pred):
    return np.mean(
        np.absolute(y_pred - y_true) / ((np.absolute(y_true) + np.absolute(y_pred)) / 2)
    )
