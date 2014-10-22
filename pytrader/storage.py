from pickle import dumps, loads

from pytrader.exceptions import RecordsNotFoundError


def _make_key_name(data, ticker):
    return "{}_{}_{}".format(data.index.values[0], data.index.values[-1], ticker)


def push_to_redis(redis, data, ticker):
    key_name = _make_key_name(data, ticker)
    redis.set(key_name, dumps(data))


def pull_from_redis(redis, ticker, start_date, end_date):
    keys = redis.keys("{}_{}_{}".format(start_date, end_date, ticker))
    if not keys:
        raise RecordsNotFoundError(ticker, start_date, end_date)
    return loads(redis.get(keys[-1]))
