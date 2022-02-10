import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture

from pfa.analytics.prophet import generate_validation_metrics


class MockMetricIDCache:
    @property
    def mean_error(self):
        return 1

    @property
    def mean_abs_error(self):
        return 2

    @property
    def rmse(self):
        return 3


@pytest.mark.parametrize(
    "true_data, predicted_data, expected_result",
    [
        pytest.param(
            pd.DataFrame([(1, 1)], columns=["ds", "y"]),
            pd.DataFrame([(1, 1)], columns=["ds", "yhat"]),
            pd.DataFrame([(1, 0.0, 0.0, 0.0)], columns=["date", 1, 2, 3]),
        )
    ],
)
def test_generate_validation_metrics(
    mocker: MockerFixture,
    true_data: pd.DataFrame,
    predicted_data: pd.DataFrame,
    expected_result: pd.DataFrame,
):
    mocker.patch("pfa.analytics.prophet.metric_id_cache", MockMetricIDCache())
    result = generate_validation_metrics(true_data, predicted_data)

    assert_frame_equal(result, expected_result)
