from datetime import datetime

from numpy import array, vectorize
from pandas import DataFrame
from ychartspy.client import YChartsClient


class YChartsDataImplementation(object):
    def __init__(self):
        self.client = YChartsClient()

    def get_prices(self, ticker, time_length):
        raw_data = self.client.get_security_prices(ticker, time_length)
        return convert_to_pandas(raw_data, "prices")

    def get_eps(self, ticker, time_length):
        raw_data = self.client.get_security_metric(ticker, "eps_ttm", time_length)
        return convert_to_pandas(raw_data, "eps")

    def get_pe(self, ticker, time_length):
        raw_data = self.client.get_security_metric(ticker, "pe_ratio", time_length)
        return convert_to_pandas(raw_data, "pe")


def convert_to_pandas(raw_data, column):
    numpy_data = array(raw_data)
    vectorized = vectorize(lambda x: datetime.fromtimestamp(x / 1000).strftime("%Y-%m-%d"))
    return DataFrame(numpy_data[:, 1], index=vectorized(numpy_data[:, 0]), columns=[column])
