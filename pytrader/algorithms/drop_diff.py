from sklearn.ensemble import RandomForestClassifier


def _calc_return(new, old):
    return (new - old) / old

def initialize(context):
    context.model = RandomForestClassifier()
    context.x = []
    context.y = []
    context.yesterday_price = {}
    context.number_days_after = 4
    context.data_points_necessary = 50
    # I don't like this.
    context.data_countdowns = []
    context.threshold = .05
    context.predictions = []


def handle_countdowns(context, data):
    countdown_idx_to_remove = []
    for idx, day_tuple in enumerate(context.data_countdowns):
        countdown = day_tuple[1] - 1
        if countdown == 0:
            # This must e a list or else RandomForest will throw an error
            context.x.append([day_tuple[3]])
            context.y.append(_calc_return(data[day_tuple[0]]["close"], day_tuple[2]) > 0)
            if day_tuple[4] is not None:
                context.predictions.append((_calc_return(data[day_tuple[0]]["close"], day_tuple[2]) > 0) == day_tuple[4])
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
                 None,  # prediction
                )
            )
            context.yesterday_price[ticker] = stock_data["close"]
        else:
            context.yesterday_price[ticker] = stock_data["close"]


def handle_data(context, data):
    handle_countdowns(context, data)
    old_data_counts = len(context.data_countdowns)
    handle_price_histories(context, data)
    new_data_counts = len(context.data_countdowns)
    if len(context.x) > context.data_points_necessary and new_data_counts > old_data_counts:
        context.model.fit(context.x, context.y)
        new_counts = context.data_countdowns[old_data_counts:]
        for idx, data_tuple in enumerate(new_counts):
            prediction = context.model.predict([data_tuple[3]])
            tmp_tuple = context.data_countdowns[old_data_counts + idx]
            tmp_tuple =  tmp_tuple[:-1] + (bool(prediction),)
            context.data_countdowns[old_data_counts + idx] = tmp_tuple
    # DEBUG for now
    if data["IBM"]["dt"].strftime("%Y-%m-%d") == "2012-12-31":
        import pdb; pdb.set_trace()
        print "Number correct:", len(filter(lambda x: x, context.predictions))
        print "Number incorrect:", len(filter(lambda x: not x, context.predictions))
        print "Total", len(context.predictions)
        import pdb; pdb.set_trace()
