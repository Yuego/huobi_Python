from AccountHandler import *
from StrategyHandler import *
import sched
import time
from datetime import datetime
from datetime import date
import pytz
import logging


def update_strategy():
    # strategy.get_data('btcusdt_1min_candle')
    # strategy.generate_indicator('btcusdt_1min_candle', ['close'], 'btc_5min_ma', moving_average, 5)
    # strategy.generate_indicator('btcusdt_1min_candle', ['close'], 'btc_10min_ma', moving_average, 10)
    # strategy.generate_signal('5min_10min_cross_up', cross_over, ['btc_5min_ma', 'btc_10min_ma'])
    # strategy.generate_signal('5min_10min_cross_down', cross_over, ['btc_10min_ma', 'btc_5min_ma'])
    # strategy.get_strat_signal('test', long='5min_10min_cross_up', short='5min_10min_cross_down')
    # a = strategy.back_test('btcusdt', '1min', 'test', loss_control, -0.1)
    strategy.get_data('btcusdt_5min_candle', 'btcusdt_1min_candle')
    strategy.generate_indicator('btcusdt_5min_candle', ['close'], 'btc_5min_boll_up', boll_band_up, 5)
    strategy.generate_indicator('btcusdt_5min_candle', ['close'], 'btc_5min_ma', boll_band_low, 5)
    strategy.generate_indicator('btcusdt_5min_candle', ['close'], 'btc_5min_close', moving_average, 1)
    strategy.generate_signal('break_up_boll_5min', cross_over, ['btc_5min_close', 'btc_5min_boll_up'])
    strategy.generate_signal('break_low_ma', cross_over, ['btc_5min_ma', 'btc_5min_close'])
    strategy.get_strat_signal('test', long='break_up_boll_5min', close_long='break_low_ma')
    a = strategy.back_test('btcusdt', '1min', 'test', loss_control, -0.1)
    print('updated strategy')
    return a


def update_account():
    try:
        account.update_asset()
        account.update_balance()
        account.update_history_trades('btcusdt')
        print(account.balance)
        logging.info(account.balance)
        order_obj = account.check_latest_order()
        if order_obj:
            logging.info(
                str(order_obj.id) + order_obj.symbol + str(order_obj.amount) + str(order_obj.price) + order_obj.state +
                str(order_obj.filled_cash_amount) + str(order_obj.filled_amount) + str(order_obj.created_at) + str(order_obj.canceled_at)
                + str(order_obj.finished_at))
    except:
        print("connection error")


def run_strategy():
    update_account()
    a = update_strategy()
    now = datetime.now().strftime("%H:%M:%S")
    print("Updated Strategy at " + now)
    logging.info("Updated Strategy at " + now)

    last_candle_time = a[0].index[-1]
    last_candle_time_str = datetime.fromtimestamp(last_candle_time, local_tz).strftime('%Y-%m-%d %H:%M:%S')
    last_signal_time=a[1].index[-1]
    last_signal_time_str = datetime.fromtimestamp(last_signal_time, local_tz).strftime('%Y-%m-%d %H:%M:%S')
    print("Last Signal TimeStamp: " + last_signal_time_str)
    logging.info("Last Signal TimeStamp: " + last_signal_time_str)

    print("Last Candle TimeStamp: " + last_candle_time_str)
    logging.info("Last Candle TimeStamp: " + last_candle_time_str)

    print("Latest Position:" + str(a[0]['position'].iloc[-1]))
    logging.info("Latest Position:" + str(a[0]['position'].iloc[-1]))

    print("Latest Price:" + str(a[0]['close'].iloc[-1]))
    logging.info("Latest Price:" + str(a[0]['close'].iloc[-1]))

    target = a[0]['position'].iloc[-1] * 0.0002
    last_price = a[0]['close'].iloc[-1]
    account.adjust_position(14304500, 'btc', target, last_price*0.99)


    s.enter(30, 1, run_strategy, ())
    s.run()


local_tz = pytz.timezone("HongKong")

d1 = date.today().strftime("%Y%m%d")
logging.basicConfig(filename="Log_" + d1 + ".txt",
                    level=logging.DEBUG,
                    format='%(levelname)s: %(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S')

s = sched.scheduler(time.time, time.sleep)

strategy = Strategy('btcusdt')
logging.info("Strategy Module Started")

account = AccountHandler()
account.get_accounts()
logging.info("Accounts Module Started")

run_strategy()
