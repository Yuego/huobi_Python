from AccountHandler import *
from StrategyHandler import *
import sched
import time
from datetime import datetime
from datetime import date
import pytz
import logging
from functools import reduce


class Production():
    def __init__(self):
        self.local_tz = pytz.timezone("HongKong")
        self.s = sched.scheduler(time.time, time.sleep)
        self.strategy = Strategy('btcusdt')
        self.account = AccountHandler()
        self.account.get_accounts()
        self.production = []
        self.strategy_list = {}
        self.strategy_weight = {}
        self.latest_position = {}
        self.portfolio_position = 0.0
        d1 = date.today().strftime("%Y%m%d")
        logging.basicConfig(filename="Log_" + d1 + ".txt",
                            level=logging.DEBUG,
                            format='%(levelname)s: %(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S')
        logging.info("Production Started")

    def ma_system(self, freq, period1, period2):
        ticker = 'btcusdt_' + freq + '_candle'
        self.strategy.generate_indicator(ticker, ['open'], 'btc_close', moving_average, 1)
        self.strategy.generate_indicator(ticker, ['open'], 'btc_ma_1', moving_average, period1)
        self.strategy.generate_indicator(ticker, ['open'], 'btc_ma_2', moving_average, period2)
        self.strategy.generate_signal('ma_cross_up', cross_over, ['btc_ma_1', 'btc_ma_2'])
        self.strategy.generate_signal('ma_cross_down', cross_over, ['btc_ma_2', 'btc_ma_1'])
        self.strategy.get_strat_signal("ma_system_" + freq + "_" + str(period1) + str(period2), long='ma_cross_up',
                                       short='ma_cross_down')
        self.strategy_list["ma_system_" + freq + "_" + str(period1) + str(period2)] \
            = self.strategy.back_test('btcusdt', freq, "ma_system_" + freq + "_" + str(period1) + str(period2),
                                      loss_control, -0.1)
        now = datetime.now().strftime("%H:%M:%S")
        print('updated strategy:ma_system_' + freq + "_" + str(period1) + str(period2) + " at" + now)

    def update_account(self):
        try:
            self.account.update_asset()
            self.account.update_balance()
            self.account.update_history_trades('btcusdt')
            print("Account Balance:")
            print(self.account.balance)
            logging.info(self.account.balance)
        except:
            print("connection error")

    def log_strategy(self, strategy_name):
        production = self.strategy_list[strategy_name]
        last_candle_time = production[0].index[-1]
        last_candle_time_str = datetime.fromtimestamp(last_candle_time, self.local_tz).strftime('%Y-%m-%d %H:%M:%S')
        last_signal_time = production[1].index[-1]
        last_signal_time_str = datetime.fromtimestamp(last_signal_time, self.local_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(strategy_name + " Last Signal TimeStamp: " + last_signal_time_str)
        logging.info(strategy_name + " Last Signal TimeStamp: " + last_signal_time_str)
        print(strategy_name + " Last Candle TimeStamp: " + last_candle_time_str)
        logging.info(strategy_name + " Last Candle TimeStamp: " + last_candle_time_str)
        self.latest_position[strategy_name] = production[0]['position'].iloc[-1]
        print(strategy_name + " Latest Position:" + str(production[0]['position'].iloc[-1]))
        logging.info(strategy_name + " Latest Position:" + str(production[0]['position'].iloc[-1]))
        print(strategy_name + " Latest Price:" + str(production[0]['open'].iloc[-1]))
        logging.info(strategy_name + " Latest Price:" + str(production[0]['open'].iloc[-1]))

    def get_strategy_weight(self):
        for item in self.strategy_list:
            self.strategy_weight[item] = 1 / len(self.strategy_list)

    def get_portfolio_position(self):
        position = 0.0
        for item in self.strategy_list:
            position += self.latest_position[item] * self.strategy_weight[item]
        self.portfolio_position = position

    def get_portfolio(self):
        position_list = []
        pnl_list = []
        price_list = []
        for item in self.strategy_list:
            position = self.strategy_list[item][0][['position']]
            position.columns = [item + 'position']
            pnl = self.strategy_list[item][0][['pnl']]
            pnl.columns = [item + 'pnl']
            price = self.strategy_list[item][0][['open']]
            price.columns = [item + 'price']
            position_list.append(position)
            pnl_list.append(pnl)
            price_list.append(price)
        position = reduce(lambda x, y: pd.merge(x, y, on='id', how='outer'), position_list)
        position = position.sort_index()
        position = position.fillna(method='ffill')
        position['average_position'] = position.mean(axis=1)

        pnl = reduce(lambda x, y: pd.merge(x, y, on='id', how='outer'), pnl_list)
        pnl = pnl.sort_index()
        pnl = pnl.fillna(0)
        pnl['average_pnl'] = pnl.mean(axis=1)

        price = reduce(lambda x, y: pd.merge(x, y, on='id', how='outer'), price_list)
        price = price.sort_index()
        price['average_price'] = price.mean(axis=1)
        portfolio=pd.DataFrame(columns=['position'])
        portfolio.loc[:,'position']=position['average_position'].values
        portfolio.loc[:,'pnl']=pnl['average_pnl'].values
        portfolio.loc[:,'price']=price['average_price'].values
        portfolio.index=position.index
        return portfolio

    def evaluate_strategy(self):
        for item in self.strategy_list:
            data = self.strategy_list[item][0]
            number_trades = sum(data['position'].diff() != 0)
            print(item + " Est Fee:" + str(number_trades * 0.002))
            print(item + " Total Return:" + str(round(data['pnl'].sum(), 2)))
            print(item + " Sharp Ratio:" + str(round(data['pnl'].sum() / data['pnl'].cumsum().std(), 2)))

    def rebalance_position(self, full_size):
        target = max(0.0, self.portfolio_position) * full_size
        last_price = self.strategy.data_list['btcusdt_1min_candle']['open'].iloc[-1]
        print("target position:" + str(target))
        print("last price:" + str(last_price))
        self.account.adjust_position(14304500, 'btc', target, last_price * 0.99)

    def run_strategy(self):
        self.update_account()
        self.strategy.get_data(8000, 'btcusdt_1day_candle', 'btcusdt_60min_candle', 'btcusdt_30min_candle',
                               'btcusdt_15min_candle', 'btcusdt_5min_candle', 'btcusdt_1min_candle')
        if len(self.strategy.data_list['btcusdt_1min_candle']) > 0:
            self.ma_system("1day", 20, 30)
            self.ma_system("1day", 10, 20)
            self.ma_system("60min", 10, 20)
            for item in self.strategy_list:
                self.log_strategy(item)
            self.get_strategy_weight()
            self.get_portfolio_position()
            print("Portfolio Position:" + str(self.portfolio_position))
            self.rebalance_position(0.001)
            self.s.enter(60, 1, self.run_strategy, ())
        else:
            self.s.enter(5, 1, self.run_strategy, ())

    def run(self):
        to_sleep_1min = 60 - time.time() % 60
        self.s.enter(to_sleep_1min, 1, self.run_strategy, ())
        self.s.run()
