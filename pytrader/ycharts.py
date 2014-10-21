from datetime import datetime

from numpy import array, vectorize
from pandas import DataFrame
from ychartspy.client import YChartsClient


class YChartsDataImplementation(object):
    def __init__(self, **kwargs):
        """
        :username: YCharts username
        :password: YCharts password
        """
        if "password" not in kwargs or "username" not in kwargs:
            raise Exception("You must pass in a ycharts username and password")
        self.client = YChartsClient()
        self.client.authenticate(kwargs["username"], kwargs["password"])

    def get_prices(self, ticker, time_length):
        raw_data = self.client.get_security_prices(ticker, time_length)
        return convert_to_pandas(raw_data, "prices")

    def get_metric(self, ticker, metric, time_length):
        raw_data = self.client.get_security_metric(ticker, metric, time_length)
        return convert_to_pandas(raw_data, metric)


def convert_to_pandas(raw_data, column):
    numpy_data = array(raw_data)
    vectorized = vectorize(lambda x: datetime.fromtimestamp(x / 1000).strftime("%Y-%m-%d"))
    return DataFrame(numpy_data[:, 1], index=vectorized(numpy_data[:, 0]), columns=[column])
