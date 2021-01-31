import sqlite3
import pandas as pd
import numpy as np


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


def loss_control(pnl, threshold):
    if len(pnl) < 30:
        drop = np.sum(pnl)
    else:
        drop = np.sum(pnl[-30:])
    if drop < threshold:
        return True
    else:
        return False


class Strategy:
    def __init__(self, symbol):
        self.symbol = symbol
        self.conn = sqlite3.connect('market.db', timeout=10)
        self.data_list = {}
        self.indicator_list = {}
        self.signal_list = {}
        self.strategy_signal_list = {}

    def get_data(self, period, *args):
        for arg in args:
            self.data_list[arg] = pd.read_sql('select * from ' + arg, self.conn).drop_duplicates(subset=['id'],
                                                                                                 keep='last').tail(
                period)

    def get_data_frame(self, table, column):
        data = self.data_list[table]
        data = data.set_index('id')
        return data[column].copy()

    def generate_indicator(self, table, column, name, indicator_func, *args):
        raw_data = self.get_data_frame(table, column)
        self.indicator_list[name] = indicator_func(raw_data, *args)

    def generate_signal(self, name, signal_func, indicators):
        args = []
        for item in indicators:
            args.append(self.indicator_list[item])
        self.signal_list[name] = signal_func(*args)

    def get_strat_signal(self, stratname, **signals):
        signal_matrix = pd.DataFrame()
        for item in signals:
            signal = self.signal_list[signals[item]]
            signal.columns = [item]
            signal_matrix = pd.concat([signal_matrix, signal], axis=1)
        columns = ['long', 'short', 'close_long', 'close_short', 'close_all']
        for idx in columns:
            if idx not in signal_matrix.columns:
                signal_matrix[idx] = 0
        self.strategy_signal_list[stratname] = signal_matrix

    def get_position(self, price, rt, signal, loss_control_func, *args):
        signal_matrix = self.strategy_signal_list[signal]
        position = []
        pnl = []
        loss_control = []
        for i in range(len(price)):
            if i == 0:
                last_position = 0
                current_position = 0
                pl = 0
                control = 0
            else:
                last_position = position[-1]
                last_rt = rt.iloc[i]
                pl = last_position * last_rt
                control = loss_control_func(pnl, *args)
                current_time = price.index[i]
                last_signal_index = ((signal_matrix.index - current_time) <= 0).sum() - 1
                last_signals = signal_matrix.iloc[last_signal_index]
                ls = last_signals['long'] * (last_signals['long'] - last_signals['close_long']) - last_signals[
                    'short'] * (last_signals['short'] - last_signals['close_short'])
                close_all = last_signals['close_long'] * last_signals['close_short'] + last_signals[
                    'close_all'] + control
                close_ls = (last_signals['close_short'] - last_signals['close_long'])
                if close_all:
                    current_position = 0
                elif ls:
                    current_position = ls

                elif (last_position > 0) & (close_ls < 0):
                    current_position = 0
                elif (last_position < 0) & (close_ls > 0):
                    current_position = 0
                else:
                    current_position = last_position

            position.append(current_position)
            pnl.append(pl)
            loss_control.append(control)

        price['position'] = position
        price['pnl'] = pnl
        price['loss_control'] = loss_control
        return [price, signal_matrix]

    def get_return(self, ticker, freq, method):
        if method == "typical":
            price = self.get_data_frame(ticker + "_" + freq + "_candle", ['open', 'close', 'high', 'low'])
            return price.mean(axis=1).pct_change(1)
        else:
            return self.get_data_frame(ticker + "_" + freq + "_candle", [method]).pct_change(1)[method]

    def back_test(self, ticker, freq, signal, loss_control_func, *args):
        price = self.get_data_frame(ticker + "_" + freq + "_candle", ['open'])
        rt = self.get_return(ticker, freq, "open")
        position = self.get_position(price.copy(), rt.copy(), signal, loss_control_func, *args)
        position.append(rt)
        return position

#
# strat = Strategy('btcusdt')
# strat.get_data('btcusdt_1min_candle')
# strat.generate_indicator('btcusdt_1min_candle', ['close'], 'btc_5min_ma', moving_average, 5)
# strat.generate_indicator('btcusdt_1min_candle', ['close'], 'btc_10min_ma', moving_average, 10)
# strat.generate_signal('5min_10min_cross_up', cross_over, ['btc_5min_ma', 'btc_10min_ma'])
# strat.generate_signal('5min_10min_cross_down', cross_over, ['btc_10min_ma', 'btc_5min_ma'])
# strat.get_strat_signal('test', long='5min_10min_cross_up', short='5min_10min_cross_down')
# a = strat.back_test('btcusdt', '1min', 'test', loss_control, -0.1)
