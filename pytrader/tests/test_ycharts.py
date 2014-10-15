from datetime import datetime
from time import mktime

from mock import Mock
from nose.tools import eq_
from pandas import DataFrame

from pytrader.ycharts import DataImplementation

RESPONSE = [
    [mktime(datetime(2014, 10, 10).timetuple()) * 1000, 10],
    [mktime(datetime(2014, 10, 11).timetuple()) * 1000, 11]
]


def test_get_prices():
    d = DataImplementation()
    mock_ycharts = Mock()
    d.client = mock_ycharts
    mock_ycharts.get_security_prices.return_value = RESPONSE
    results = d.get_prices("FOO", 1)
    eq_(results.to_csv(), DataFrame([10.0, 11.0], index=["2014-10-10", "2014-10-11"]).to_csv())
