"""
Since zipline is taking its sweet time to download data;
I am going to just have do this whole thing myself
"""
import os

from finsymbols import get_sp500_symbols
from matplotlib import dates
from matplotlib import pyplot as plt
import numpy as np
import quandl

from pytrader.algorithms.current_movers import (
    get_company_key_data_by_last_x_and_date, get_first_date_by_start, get_sym_data
)


def initialize(last_x, date):
    key = os.getenv("QUANDL_API_KEY")
    if not key:
        raise Exception("Must input a Quandl API key as QUANDL_API_KEY")
    quandl.ApiConfig.api_key = key
    sp500_data = get_sym_data("INDEX_GSPC")
    sp500_syms = map(lambda x: x['symbol'], get_sp500_symbols())
    rows = []
    for sym in sp500_syms:
        row = get_company_key_data_by_last_x_and_date(sym, 100, sp500_data, date)
        if row:
            rows.append(row)
    return rows


def handle_data(rows, start_date, end_date, index_data):
    by_max_return = sorted(rows, key=lambda x: x[1])
    by_max_median_returns = sorted(rows, key=lambda x: x[2])
    top_ten = by_max_median_returns[-10:]
    syms = map(lambda x: x[0], top_ten)
    history = {sym: {"buy": 0, "sell": 0, "shares": 1} for sym in syms}
    for sym in syms:
        data = get_sym_data(sym)
        sym_start = get_first_date_by_start(data, start_date)
        sym_end = get_first_date_by_start(data, end_date)
        history[sym]["buy"] = data.loc[sym_start]["Adj. Close"]
        history[sym]["sell"] = data.loc[sym_end]["Adj. Close"]
    start_value = 0
    total_value = 0
    for sym in syms:
        shares = history[sym]["shares"]
        start_value += history[sym]["buy"] * shares
        total_value += history[sym]["sell"] * shares
    final_return = (total_value - start_value) / start_value
    index_start = index_data.loc[start_date]["Adjusted Close"]
    index_end = index_data.loc[end_date]["Adjusted Close"]
    index_return = (index_end - index_start) / index_start
    return index_return, final_return


def plot_results(x, index_returns, algo_returns, last_x):
    fig, ax = plt.subplots()
    ax.plot(index_returns, marker="+")
    ax.plot(algo_returns, marker="o")
    plt.xticks(range(len(x)), x)
    ax.fmt_xdata = dates.DateFormatter('%Y-%m-%d')
    fig.autofmt_xdate()
    plt.title("Returns for algo over retro by {} days".format(last_x))
    plt.show()


def main():
    index_returns = []
    algo_returns = []
    x = []
    index_data = get_sym_data("INDEX_GSPC")
    for historic_idx in range(-700, -600):
        date = str(index_data.iloc[historic_idx].name)
        x.append(date.split(" ")[0])
        last_x = 200
        rows = initialize(last_x, date)
        sell_date = str(index_data.iloc[historic_idx + 300].name)
        index_return, algo_return = handle_data(rows, date, sell_date, index_data)
        index_returns.append(index_return)
        algo_returns.append(algo_return)
    plot_results(x, index_returns, algo_returns, last_x)


if __name__ == "__main__":
    main()
