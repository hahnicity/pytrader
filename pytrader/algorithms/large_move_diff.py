import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier

from zipline.api import order_percent, order_target


def _calc_return(new, old):
    return (new - old) / old


def initialize(context):
    context.model = RandomForestClassifier()
    context.x = []
    context.y = []
    context.yesterday_price = {}
    context.number_days_after = 1
    context.data_points_necessary = 0
    context.data_countdowns = []
    context.to_terminate = []
    context.threshold = .02
    context.predictions = []


def handle_countdowns(context, data):
    countdown_idx_to_remove = []
    for idx, day_tuple in enumerate(context.data_countdowns):
        countdown = day_tuple[1] - 1
        if countdown == 0:
            # This must e a list or else RandomForest will throw an error
            context.x.append([day_tuple[3]])
            context.y.append(_calc_return(data[day_tuple[0]]["close"], day_tuple[2]) > 0)
            countdown_idx_to_remove.append(idx)
        else:
            context.data_countdowns[idx] = (day_tuple[0], countdown) + day_tuple[2:]
    context.data_countdowns = [
        day_tuple for idx, day_tuple in enumerate(context.data_countdowns)
        if idx not in countdown_idx_to_remove
    ]


def handle_price_histories(context, data):
    for ticker, stock_data in data.items():
        if ticker not in context.yesterday_price:
            context.yesterday_price[ticker] = stock_data["close"]
        elif abs(_calc_return(stock_data["close"], context.yesterday_price[ticker])) > context.threshold:
            # This should really be a namedtuple
            context.data_countdowns.append(
                (ticker,
                 context.number_days_after,
                 stock_data["close"],
                 _calc_return(stock_data["close"], context.yesterday_price[ticker]),
                )
            )
            context.yesterday_price[ticker] = stock_data["close"]
        else:
            context.yesterday_price[ticker] = stock_data["close"]


def handle_terminations(context):
    idx_to_remove = []
    for idx, position in enumerate(context.to_terminate):
        ticker = position[0]
        # This isn't correct but oh well for now
        #import pdb; pdb.set_trace()
        order_target(ticker, 0)
        idx_to_remove.append(idx)
    context.to_terminate = [
        data for idx, data in enumerate(context.to_terminate) if idx not in idx_to_remove
    ]


def handle_data(context, data):
    handle_terminations(context)
    handle_countdowns(context, data)
    old_data_counts = len(context.data_countdowns)
    handle_price_histories(context, data)
    new_data_counts = len(context.data_countdowns)
    if len(context.x) > context.data_points_necessary and new_data_counts > old_data_counts:
        context.model.fit(context.x, context.y)
        new_counts = context.data_countdowns[old_data_counts:]
        for idx, data_tuple in enumerate(new_counts):
            prediction = context.model.predict([data_tuple[3]])
            order_percent(data_tuple[0], {1: 1, 0: -1}[int(prediction)] * (1.0 / len(data)))
            # This isn't correct but oh well for now
            context.to_terminate.append((data_tuple[0], {1: 1, 0: -1}[int(prediction)]))


def analyze(context, perf):
    perf.portfolio_value.plot()
    plt.ylabel('portfolio value in $')
    plt.legend(loc=0)
    plt.show()
