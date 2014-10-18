from datetime import datetime
from pickle import dumps, loads


def _make_key_name(data, ticker, time_length):
    return "{}_{}_{}".format(data.index.values[-1], time_length, ticker)


def push_to_redis(redis, data, ticker, time_length):
    key_name = _make_key_name(data, ticker, time_length)
    redis.set(key_name, dumps(data))


def pull_latest_report_from_redis(redis, ticker, time_length):
    keys = redis.keys("*_{}_{}".format(time_length, ticker))
    return loads(redis.get(keys[-1]))
