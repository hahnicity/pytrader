"""
Serves to gather data from the internet on our stock of choice
"""
from argparse import ArgumentParser
from datetime import datetime

from redis import StrictRedis

from pytrader.gatherer import gather_data
from pytrader.storage import push_to_redis
from pytrader.ycharts import YChartsDataImplementation


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", default=6379, type=int)
    parser.add_argument("--redis-db", default=0, type=int)
    parser.add_argument("--pool-size", default=20, type=int)
    parser.add_argument("--num-calls-per-batch", default=1000, type=int)
    parser.add_argument("--seconds-per-batch", default=300, type=int)
    parser.add_argument("--batch-poll-timeout", default=2, type=int)
    parser.add_argument("-t", "--ticker", required=True)
    parser.add_argument("--time-length", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    redis = StrictRedis(host=args.redis_host, port=args.redis_port, db=args.redis_db)
    data = gather_data(args, YChartsDataImplementation)
    push_to_redis(redis, data, args.ticker, args.time_length)
