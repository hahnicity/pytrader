from collections import namedtuple
from getpass import getpass

import matplotlib.pyplot as plt
from numpy import append, array, diff
from pandas import DataFrame
from redis import StrictRedis
from sklearn.ensemble import RandomForestClassifier
from zipline.api import order_percent, order_target

from pytrader.exceptions import RecordsNotFoundError
from pytrader.gatherer import gather_data_with_single_process_client
from pytrader.main import get_authenticated_data_impl
from pytrader.storage import pull_from_redis, push_to_redis


def _calc_return(new, old):
    return (new - old) / old


def get_x_point(context, data, ticker, move_return):
    date = data[ticker]["dt"].strftime("%Y-%m-%d")
    pytrader_data = context.pytrader_data[ticker].loc[date].values
    return append(pytrader_data, move_return)


def initialize(context):
    context.pytrader_data = {}
    context.model = RandomForestClassifier()
    context.StockTuple = namedtuple("StockTuple", ["ticker", "days_after", "close", "move_return"])
    context.x = []
    context.y = []
    context.yesterday_price = {}
    context.number_days_after = 1
    context.data_points_necessary = 50
    context.data_countdowns = []
    context.to_terminate = []
    context.threshold = .05
    context.predictions = []


def calculate_diffs(df):
    diff_cols = ["diff_{}".format(key) for key in df.keys()]
    diff_vals = [append(0, diff(df[key].values) / df[key].values[:-1]) for key in df.keys()]
    return DataFrame(array(diff_vals).transpose(), index=df.index.values, columns=diff_cols)


def post_initialize(context, data):
    """
    Since `initialize` doesn't actually enable us to see the parameters of when
    our simulation is starting/ending we need to create a `post_initialize` func
    to handle gathering extra data from pytrader
    """
    if context.sim_params.period_start == data[data.keys()[0]]["dt"]:
        redis = StrictRedis(host="localhost", port=6379, db=0)
        data_impl = get_authenticated_data_impl("grehm87@gmail.com", getpass())
        # Since we are currently using YCharts our data schema is a little weird
        start_date = context.sim_params.period_start.strftime("%Y-%m-%d")
        end_date = context.sim_params.period_end.strftime("%Y-%m-%d")
        for ticker in data.keys():
            try:
                pytrader_data = pull_from_redis(redis, ticker, start_date, end_date)
            except RecordsNotFoundError:
                pytrader_data = gather_data_with_single_process_client(data_impl, ticker, None, start_date, end_date)
                push_to_redis(redis, pytrader_data, ticker)
            del pytrader_data["price"]  # Delete the price col, we don't need it
            pytrader_data = calculate_diffs(pytrader_data)
            context.pytrader_data[ticker] = pytrader_data


def handle_countdowns(context, data):
    countdown_idx_to_remove = []
    for idx, stock_tuple in enumerate(context.data_countdowns):
        ticker = stock_tuple.ticker
        countdown = stock_tuple.days_after - 1
        if countdown == 0:
            context.x.append(get_x_point(context, data, stock_tuple.ticker, stock_tuple.move_return))
            context.y.append(_calc_return(data[stock_tuple.ticker]["close"], stock_tuple.close) > 0)
            countdown_idx_to_remove.append(idx)
        else:
            context.data_countdowns[idx] = context.StockTuple(
                stock_tuple.ticker, countdown, *stock_tuple[2:]
            )
    context.data_countdowns = [
        stock_tuple for idx, stock_tuple in enumerate(context.data_countdowns)
        if idx not in countdown_idx_to_remove
    ]


def handle_price_histories(context, data):
    for ticker, stock_data in data.items():
        if ticker not in context.yesterday_price:
            context.yesterday_price[ticker] = stock_data["close"]
        elif abs(_calc_return(stock_data["close"], context.yesterday_price[ticker])) > context.threshold:
            context.data_countdowns.append(context.StockTuple(
                ticker,
                context.number_days_after,
                stock_data["close"],
                _calc_return(stock_data["close"], context.yesterday_price[ticker])
            ))
            context.yesterday_price[ticker] = stock_data["close"]
        else:
            context.yesterday_price[ticker] = stock_data["close"]


def handle_terminations(context):
    idx_to_remove = []
    for idx, position in enumerate(context.to_terminate):
        ticker = position[0]
        countdown = position[1] - 1
        if countdown == 0:
            order_target(ticker, 0)
            idx_to_remove.append(idx)
        else:
            context.to_terminate[idx] = (ticker, countdown)
    context.to_terminate = [
        data for idx, data in enumerate(context.to_terminate) if idx not in idx_to_remove
    ]


def handle_data(context, data):
    post_initialize(context, data)
    handle_terminations(context)
    handle_countdowns(context, data)
    old_data_counts = len(context.data_countdowns)
    handle_price_histories(context, data)
    new_data_counts = len(context.data_countdowns)
    if len(context.x) > context.data_points_necessary and new_data_counts > old_data_counts:
        context.model.fit(context.x, context.y)
        new_counts = context.data_countdowns[old_data_counts:]
        for idx, stock_tuple in enumerate(new_counts):
            prediction = context.model.predict(
                get_x_point(context, data, stock_tuple.ticker, stock_tuple.move_return)
            )
            order_percent(stock_tuple.ticker, {1: 1, 0: -1}[int(prediction)] * (1.0 / len(data)))
            context.to_terminate.append((stock_tuple.ticker, context.number_days_after))


def analyze(context, perf):
    perf.portfolio_value.plot()
    plt.ylabel('portfolio value in $')
    plt.legend(loc=0)
    plt.show()
