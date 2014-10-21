"""
Serves to gather data from the internet on our stock of choice
"""
from argparse import ArgumentParser

from cowboycushion.multiprocessing_limiter import RedisMultiprocessingLimiter
from redis import StrictRedis

from pytrader.gatherer import gather_data
from pytrader.storage import push_to_redis
from pytrader.ycharts import YChartsDataImplementation


def get_data_impl(user, password):
    data_impl = YChartsDataImplementation(username=user, password=password)
    return data_impl


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
    parser.add_argument("--ycharts-user", required=True)
    parser.add_argument("--ycharts-pw", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    redis = StrictRedis(host=args.redis_host, port=args.redis_port, db=args.redis_db)
    data_impl = get_data_impl(args.ycharts_user, args.ycharts_pw)
    data_client = RedisMultiprocessingLimiter(
        data_impl,
        args.batch_poll_timeout,
        args.num_calls_per_batch,
        args.seconds_per_batch,
        args.redis_host,
        args.redis_port,
        args.redis_db,
        args.pool_size
    )
    # Just keep start/end date at None/None for now
    data = gather_data(data_client, args.ticker, args.time_length, None, None)
    import pdb; pdb.set_trace()
    push_to_redis(redis, data, args.ticker)
