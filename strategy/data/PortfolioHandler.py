import sqlite3
import pandas as pd
from huobi.client.market import MarketClient
import numpy as np


def summary(position, close):
    pnl = calculate_pnl(position, close)
    pnl.index = pd.to_datetime(pnl.index.astype(int), unit='s')
    pnl.plot()
    position.index = pd.to_datetime(position.index.astype(int), unit='s')
    return position.loc[:, position.tail(10).any()].tail(10)


def relative_strength(c, parameter):
    rs = c.diff()[c.diff() > 0].fillna(0).rolling(parameter).sum().fillna(0) / -c.diff()[c.diff() < 0].fillna(
        0).rolling(
        parameter).sum()
    return rs


def moving_average(data, parameter):
    return data.rolling(parameter).mean()


def boll_band_up(data, parameter):
    return data.rolling(parameter).mean() + data.rolling(parameter).std()


def boll_band_up_2(data, parameter):
    return data.rolling(parameter).mean() + 2 * data.rolling(parameter).std()


def boll_band_low(data, parameter):
    return data.rolling(parameter).mean() - data.rolling(parameter).std()


def boll_band_low_2(data, parameter):
    return data.rolling(parameter).mean() - 2 * data.rolling(parameter).std()


def cross_over(indicator1, indicator2):
    return (((indicator1 > indicator2) * 1.0).diff() > 0.0) * 1.0


def rank_matrix(matrix):
    return matrix.rank(axis=1, numeric_only=True, na_option='keep', ascending=False, pct=True)


def rank_to_signal(matrix, threshold):
    matrix = matrix.rank(axis=1, numeric_only=True, na_option='keep', ascending=True)
    signal = (matrix <= threshold) * 1.0
    return signal


def signal_to_position(buysignal, sellsignal, method='default'):
    if method == 'default':
        position = (buysignal - sellsignal).replace(0, method='ffill')
        position = position.replace(-1, 0)
    if method == 'longonly_cum':
        position = (buysignal - sellsignal).replace(0, method='ffill')
        position = position.replace(-1, 0)
        position = buysignal.cumsum() * position
    if method == 'long_short':
        position = (buysignal - sellsignal).replace(0, method='ffill')
    if method == 'long_short_cum':
        temp = (buysignal - sellsignal).replace(0, method='ffill')
        position = buysignal.cumsum() * (temp > 0) - sellsignal.cumsum() * (temp < 0)
    if method == 'pure_cum':
        position = (buysignal - sellsignal).cumsum()
    return position


def rank_to_position(matrix, threshold, holding_period):
    matrix = matrix.rank(axis=1, numeric_only=True, na_option='keep', ascending=True)
    position = ((matrix <= threshold) - (matrix <= threshold).shift(holding_period).fillna(0)).cumsum()
    position = (position > 0) * 1.0
    return position


def matrix_rt(matrix):
    return matrix.pct_change(1).fillna(0)


def volatility(matrix, period):
    return matrix.rolling(period).std()


def net_inflow(h, l, c, v, period):
    t = (h + l + c) / 3
    mf = v * t
    inflow = (t.diff() > 0) * mf
    outflow = (t.diff() < 0) * mf
    nif = inflow.rolling(period).sum() - outflow.rolling(period).sum()
    return nif


def calculate_pnl(position, close):
    rt = close.pct_change(1).fillna(0)
    pnl = ((position.shift(1).fillna(0) * rt).sum(axis=1) / abs(position.shift(1)).sum(axis=1)).cumsum()
    return pnl


class Portfolio:
    def __init__(self):
        self.conn = sqlite3.connect('market.db', timeout=10)
        self.market_client = MarketClient()
        self.data_list = {}
        self.tickers = []
        self.get_tickers()
        self.matrix_list = {}

    def get_tickers(self):
        list_obj = self.market_client.get_market_tickers()
        for obj in list_obj:
            if not obj.symbol.endswith('nav'):
                self.tickers.append(obj.symbol)

    def get_data(self, period, *args):
        for arg in args:
            self.data_list[arg] = pd.read_sql('select * from ' + arg, self.conn).drop_duplicates(subset=['id'],
                                                                                                 keep='last').tail(
                period)

    def get_data_list(self, len):
        for ticker in self.tickers:
            name = ticker + '_1day_candle'
            try:
                self.get_data(len, name)
            except:
                pass

    def get_data_frame(self, name, column):
        table = name + '_1day_candle'
        data = self.data_list[table]
        data = data.set_index('id')
        data = data[column].copy()
        data.name = name
        return data

    def get_matrix(self, column):
        for idx, ticker in enumerate(self.tickers):
            try:
                temp = self.get_data_frame(ticker, column)
                if idx == 0:
                    data = temp
                else:
                    data = pd.merge(data, temp, how='outer', on='id')
                data = data.sort_index()
            except:
                pass
        self.matrix_list[column] = data

# p = Portfolio()
# p.get_data_list(100)
# d = p.get_matrix('vol')
# print(d)
