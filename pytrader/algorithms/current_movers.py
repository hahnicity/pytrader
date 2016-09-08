import argparse
import os
import warnings

from finsymbols import get_nasdaq_symbols, get_sp500_symbols
import numpy as np
import pandas as pd
import prettytable
import quandl
import scipy


def pretty(rows):
    table = prettytable.PrettyTable()
    table.field_names = ['sym', 'return', 'median daily return', 'variance', 'beta']
    rows = sorted(rows, key=lambda x: x[2])
    for row in rows:
        table.add_row(row)
    print(table)


def get_first_date_by_start(data, date, slice=10):
    possibles = pd.date_range(date, periods=slice)
    return str(data.loc[possibles].dropna().index[0])


def get_daily_returns(series):
    tmp = series.diff()[1:]
    tmp.index = range(len(tmp))
    tmp2 = series.copy()[:-1]
    tmp2.index = range(len(tmp2))
    return tmp / tmp2


def get_sym_data(sym):
    path = os.path.join("pickles", "{}.pickle".format(sym))
    if os.path.exists(path):
        data = pd.read_pickle(path)
    else:
        data = quandl.get("YAHOO/{}".format(sym))
        data.to_pickle(path)
    return data


def slice_by_last_x_points(asset_data, index_data, last_x):
    try:
        last_asset_points = asset_data["Adjusted Close"][-1 * last_x:]
    except:
        last_asset_points = asset_data["Adj. Close"][-1 * last_x:]
    try:
        last_index_points = index_data["Adjusted Close"][-1 * len(last_points):]
    except:
        last_index_points = index_data["Adj. Close"][-1 * len(last_points):]
    return last_asset_points, last_index_points


def slice_from_start_date(asset_data, index_data, last_x, date):
    asset_date_idx = np.where(asset_data.index==date)[0][0]
    index_date_idx = np.where(index_data.index==date)[0][0]
    if asset_date_idx - last_x < 0:
        start_idx = 0
    else:
        start_idx = asset_date_idx - last_x
    try:
        last_asset_points = asset_data["Adjusted Close"].iloc[start_idx:asset_date_idx]
    except:
        last_asset_points = asset_data["Adj. Close"].iloc[start_idx:asset_date_idx]
    try:
        last_index_points = index_data["Adjusted Close"].iloc[start_idx:asset_date_idx]
    except:
        last_index_points = index_data["Adj. Close"].iloc[start_idx:asset_date_idx]
    return last_asset_points, last_index_points


def get_key_data(sym, asset_data, index_data):
    total_return = (asset_data[-1] - asset_data[0]) / asset_data[0]
    daily_returns = get_daily_returns(asset_data)
    median_daily_return = daily_returns.median()
    variance = daily_returns.var()
    daily_index_returns = get_daily_returns(index_data)
    beta = (scipy.cov(daily_returns, daily_index_returns) / daily_index_returns.var())[0][1]
    return [
        sym,
        round(total_return, 4),
        round(median_daily_return, 5),
        round(variance, 6),
        round(beta, 4)
    ]


def get_company_key_data_by_last_x_slice(sym, last_x, index_data):
    try:
        data = get_sym_data(sym)
    except:
        return
    last_points, last_index_points = slice_by_last_x_points(data, index_data, last_x)
    return get_key_data(sym, last_points, last_index_points)


def get_company_key_data_by_last_x_and_date(sym, last_x, index_data, date):
    try:
        data = get_sym_data(sym)
    except Exception as err:
        warnings.warn(err.message)
        return
    try:
        asset_data, index_data = slice_from_start_date(data, index_data, last_x, date)
    except Exception as err:
        warnings.warn(err.message)
        return
    try:
        return get_key_data(sym, asset_data, index_data)
    except Exception as err:
        return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("api_key")
    parser.add_argument("last_x", type=int)
    subparsers = parser.add_subparsers()
    nasdaq = subparsers.add_parser("nasdaq")
    nasdaq.set_defaults(func=get_nasdaq_symbols, index="NASDAQOMX/COMP")
    sp500 = subparsers.add_parser("sp500")
    sp500.set_defaults(func=get_sp500_symbols, index="YAHOO/INDEX_GSPC")
    args = parser.parse_args()
    index_data = quandl.get(args.index)
    syms = map(lambda x: x['symbol'], args.func())
    quandl.ApiConfig.api_key = args.api_key
    rows = []
    for sym in syms:
        row = get_company_key_data_by_last_x_slice(sym, args.last_x, index_data)
        if row:
            rows.append(row)
    pretty(rows)


if __name__ == "__main__":
    main()
