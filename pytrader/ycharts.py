from datetime import datetime
import re

from numpy import array, vectorize
from pandas import DataFrame
from ychartspy.client import YChartsClient


def _convert_date(date):
    """
    Convert date from yyyy-mm-dd to mm/dd/yyy
    """
    regex = re.compile("(\d{4})-([0-1][0-9])-([0-3][0-9])")
    results = regex.search(str(date))  # convert to str to handle NoneTypes
    if not results:
        return date
    groups = results.groups()
    return "{}/{}/{}".format(groups[1], groups[2], groups[0])


class YChartsDataImplementation(object):
    def __init__(self):
        self.client = YChartsClient()

    def authenticate(self, username, password):
        self.client.authenticate(username, password)

    def get_metric(self, ticker, metric, time_length, start_date, end_date):
        start_date = _convert_date(start_date)
        end_date = _convert_date(end_date)
        raw_data = self.client.get_security_metric(
            ticker, metric, time_length=time_length, start_date=start_date, end_date=end_date
        )
        return convert_to_pandas(raw_data, metric)

    def get_prices(self, ticker, time_length, start_date, end_date):
        start_date = _convert_date(start_date)
        end_date = _convert_date(end_date)
        raw_data = self.client.get_security_prices(
            ticker, time_length=time_length, start_date=start_date, end_date=end_date
        )
        return convert_to_pandas(raw_data, "price")


def convert_to_pandas(raw_data, column):
    numpy_data = array(filter(lambda x: x[1] is not None, raw_data))
    dt_vectorized = vectorize(lambda x: datetime.utcfromtimestamp(x / 1000).strftime("%Y-%m-%d"))
    return DataFrame(numpy_data[:, 1], index=dt_vectorized(numpy_data[:, 0]), columns=[column])
