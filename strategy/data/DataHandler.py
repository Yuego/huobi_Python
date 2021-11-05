import sched
import sqlite3
import time
import time

import pandas as pd

from huobi.client.market import MarketClient
from huobi.constant import *
from huobi.utils import *


def candle_to_dict(candle):
    return {'id': candle.id, 'open': candle.open, 'close': candle.close, 'high': candle.high, 'low': candle.low,
            'amount': candle.amount, 'count': candle.count, 'vol': candle.vol}


def trade_to_dict(trade):
    return {'id': trade.ts, 'price': trade.price, 'amount': trade.amount, 'direction': trade.direction}


def parse_candle_list(list_obj):
    data = pd.DataFrame(columns=['id', 'open', 'close', 'high', 'low', 'amount', 'count', 'vol'])
    for item in list_obj:
        data = data.append(candle_to_dict(item), ignore_index=True)
    return data


def parse_trade_list(list_obj):
    data = pd.DataFrame(columns=['id', 'price', 'amount', 'direction'])
    for item in list_obj:
        data = data.append(trade_to_dict(item), ignore_index=True)
    return data


def error(e: 'HuobiApiException'):
    print(e.error_code + e.error_message)


class DataHandler:
    def __init__(self):
        self.market_client = MarketClient()
        self.candle_tick = pd.DataFrame(columns=['id', 'open', 'close', 'high', 'low', 'amount', 'count', 'vol'])
        self.mbp_tick = pd.DataFrame(
            columns=["id", "prevSeqNum", "bids_0_price", "bids_1_price", "bids_2_price", "bids_3_price",
                     "bids_4_price",
                     "asks_0_price", "asks_1_price", "asks_2_price", "asks_3_price", "asks_4_price",
                     "asks_0_amount", "asks_1_amount", "asks_2_amount", "asks_3_amount", "asks_4_amount",
                     "bids_0_amount", "bids_1_amount", "bids_2_amount", "bids_3_amount", "bids_4_amount"])
        self.trade_tick = pd.DataFrame(columns=['id', 'price', 'amount', 'direction'])
        self.tickers = []
        self.conn = sqlite3.connect('market.db', timeout=10)
        self.s = sched.scheduler(time.time, time.sleep)

    def get_tickers(self):
        self.tickers = []
        list_obj = self.market_client.get_market_tickers()
        for obj in list_obj:
            self.tickers.append(obj.symbol)

    def get_candle_list(self, interval, length, symbol):
        list_obj = self.market_client.get_candlestick(symbol, interval, length)
        return list_obj

    def get_trade_list(self, length, symbol):
        list_obj = self.market_client.get_history_trade(symbol, length)
        return list_obj

    def get_candle(self, interval, length, symbol):
        list_obj = self.get_candle_list(interval, length, symbol)
        data = parse_candle_list(list_obj)
        return data

    def get_trade(self, length, symbol):
        list_obj = self.get_trade_list(length, symbol)
        data = parse_trade_list(list_obj)
        return data

    def update_database(self, table, data):
        c = self.conn.cursor()
        # get the count of tables with the name
        c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" + table + "'")
        # #if the count is 1, then table exists
        if c.fetchone()[0] == 1:
            last_data = pd.read_sql('select * from ' + table, self.conn)
            last_data = last_data.append(data).drop_duplicates().sort_values(by='id', ascending=True)
            last_data.sort_values(by='id', ascending=True).to_sql(table, self.conn, if_exists='replace', index=False)
        else:
            data.sort_values(by='id', ascending=True).to_sql(table, self.conn, if_exists='replace', index=False)

    def update_candle(self, interval, freq, symbol):
        candle = self.get_candle(interval, freq, symbol)
        self.update_database(symbol + '_' + interval + '_' + 'candle', candle)

    def update_trade(self, freq, symbol):
        trade = self.get_trade(freq, symbol)
        self.update_database(symbol + '_' + 'trade', trade)

    def candle_callback(self, candlestick_event: 'CandlestickEvent'):
        candle = candlestick_event.return_object()
        self.candle_tick = self.candle_tick.append(candle, ignore_index=True)
        # print(self.candle_tick)

    def mbp_callback(self, mbp_event: 'MbpFullEvent'):
        mbp = mbp_event.return_object()
        self.mbp_tick = self.mbp_tick.append(mbp, ignore_index=True)
        # print(self.mbp_tick)

    def trade_callback(self, trade_event: 'TradeDetailEvent'):
        obj_list = trade_event.return_object()
        for trade in obj_list:
            self.trade_tick = self.trade_tick.append(trade_to_dict(trade), ignore_index=True)
        # print(self.trade_tick)

    def candle_subscribe(self, interval, symbol):
        self.market_client.sub_candlestick(symbol, interval, self.candle_callback, error)

    # def mbp_subscribe(self, symbol):
    #     self.market_client.sub_mbp_full(symbol, MbpLevel.MBP5, self.mbp_callback, error)

    def trade_subscribe(self, symbol):
        self.market_client.sub_trade_detail(symbol, self.trade_callback, error)

    def save_tick_data(self, symbol):
        self.update_database(symbol + "_tick_trade", self.trade_tick)
        self.update_database(symbol + "_tick_mbp", self.mbp_tick)
        self.update_database(symbol + "_tick_candle", self.candle_tick)
        print("saved tick data")
        self.s.enter(60, 1, self.save_tick_data, ())

    def save_1min_candle(self):
        try:
            self.update_candle("1min", 10)
        except:
            print("connection error")
        to_sleep_1min = 60 - time.time() % 60
        print(to_sleep_1min)
        self.s.enter(to_sleep_1min + 1, 1, self.save_1min_candle, ())

    def save_5min_candle(self):
        try:
            self.update_candle("5min", 10)
            print("saved 5min candle")
        except:
            print("connection error")
        to_sleep_5min = 300 - time.time() % 300
        self.s.enter(to_sleep_5min + 1, 1, self.save_5min_candle, ())

    def save_15min_candle(self):
        try:
            self.update_candle("15min", 10)
            print("saved 15min candle")
        except:
            print("connection error")
        to_sleep_15min = 900 - time.time() % 900
        self.s.enter(to_sleep_15min + 1, 1, self.save_15min_candle, ())

    def save_30min_candle(self):
        try:
            self.update_candle("30min", 10)
            print("saved 30min candle")
        except:
            print("connection error")
        to_sleep_30min = 1800 - time.time() % 1800
        self.s.enter(to_sleep_30min + 1, 1, self.save_30min_candle, ())

    def save_60min_candle(self):
        self.get_tickers()
        for idx, symbol in enumerate(self.tickers):
            print(str(idx) + " " + symbol)
            try:
                # self.update_candle("60min", 10, symbol)
                self.update_candle("1day", 2000, symbol)
                # print("saved 60min candle")
            except:
                print("connection error")
        print("saved 1day candle")
        to_sleep_60min = 3600 - time.time() % 3600
        self.s.enter(to_sleep_60min + 1, 1, self.save_60min_candle, ())

    # def save_1day_candle(self):
    #     try:
    #         self.update_candle("1day", 10)
    #         print("saved 1day candle")
    #     except:
    #         print("connection error")
    #     self.s.enter(86400, 1, self.save_1day_candle, ())

    def save_trade_data(self):
        try:
            self.update_trade(100)
            print("saved trade data")
        except:
            print("connection error")
        self.s.enter(10, 1, self.save_trade_data, ())

    def update_candle_data(self):
        # for idx, symbol in enumerate(self.tickers):
        #     print(str(idx)+" "+symbol)
        #     try:
        #         self.update_trade(100, symbol)
        #     except:
        #         print("wrong ticker")
        # self.s.enter(0, 1, self.save_trade_data, ())
        # self.s.enter(0, 1, self.save_1min_candle, ())
        # self.s.enter(0, 1, self.save_5min_candle, ())
        # self.s.enter(0, 1, self.save_15min_candle, ())
        # self.s.enter(0, 1, self.save_30min_candle, ())
        self.s.enter(0, 1, self.save_60min_candle, ())
        self.s.run()

    def update_tick_data(self):
        self.trade_subscribe()
        self.candle_subscribe("1min")
        self.mbp_subscribe()
        self.s.enter(60, 1, self.save_tick_data, ())


m = DataHandler()
m.update_candle_data()

# m.update_data("ethusdt", '1min')
# conn = sqlite3.connect('market.db', timeout=10)
# table = 'ethusdt_trade'
# data = pd.read_sql('select * from ' + table, conn)
# print(data)
