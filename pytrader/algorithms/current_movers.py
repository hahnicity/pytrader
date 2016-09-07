import argparse
import os

from finsymbols import get_nasdaq_symbols, get_sp500_symbols
import pandas as pd
import prettytable
import quandl


def pretty(rows):
    table = prettytable.PrettyTable()
    table.field_names = ['sym', 'gain', 'average gain', 'variance']
    rows = sorted(rows, key=lambda x: x[1])
    for row in rows:
        table.add_row(row)
    print(table)


def get_company_key_data(sym, last_x):
    path = os.path.join("pickles", "{}.pickle".format(sym))
    if os.path.exists(path):
        data = pd.read_pickle(path)
    else:
        try:
            data = quandl.get("YAHOO/{}".format(sym))
        except:
            return
        data.to_pickle(path)
    try:
        last_points = data["Adjusted Close"][-1 * last_x:]
    except:
        last_points = data["Adj. Close"][-1 * last_x:]
    percent_gain = (last_points[-1] - last_points[0]) / last_points[0]
    average_gain = last_points.diff().mean()
    variance = last_points.diff().var()
    return [sym, percent_gain, average_gain, variance]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("api_key")
    parser.add_argument("last_x", type=int)
    args = parser.parse_args()
    nasdaq = map(lambda x: x['symbol'], get_nasdaq_symbols())
    quandl.ApiConfig.api_key = args.api_key
    rows = []
    for sym in nasdaq:
        row = get_company_key_data(sym, args.last_x)
        if row:
            rows.append(row)
    pretty(rows)


if __name__ == "__main__":
    main()
