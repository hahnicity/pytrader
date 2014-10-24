from datetime import datetime

from numpy import array, vectorize
from pandas import DataFrame
from ychartspy.client import YChartsClient


class YChartsDataImplementation(object):
    def __init__(self):
        self.client = YChartsClient()

    def authenticate(self, username, password):
        self.client.authenticate(username, password)

    def get_metric(self, ticker, metric, time_length, start_date, end_date):
        raw_data = self.client.get_security_metric(
            ticker, metric, time_length=time_length, start_date=start_date, end_date=end_date
        )
        return convert_to_pandas(raw_data, metric)


def convert_to_pandas(raw_data, column):
    numpy_data = array(raw_data)
    vectorized = vectorize(lambda x: datetime.fromtimestamp(x / 1000).strftime("%Y-%m-%d"))
    return DataFrame(numpy_data[:, 1], index=vectorized(numpy_data[:, 0]), columns=[column])
