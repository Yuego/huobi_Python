from huobi.client.trade import TradeClient
from huobi.client.algo import AlgoClient
from huobi.constant import *
from huobi.utils import *

import pandas as pd


class TradeHandler:
    def __init__(self):
        self.api_key = "db8f342a-3540b528-hrf5gdfghe-5e793"
        self.secret_key = "366684d9-94c5fdf7-ad3b02a0-446bf"
        self.trade_client = TradeClient(api_key=self.api_key, secret_key=self.secret_key)
        self.algo_client = AlgoClient(api_key=self.api_key, secret_key=self.secret_key)

    def get_feerate(self, symbol):
        list_obj = self.trade_client.get_feerate(symbols=symbol)
        LogInfo.output_list(list_obj)

    def get_history_orders(self, symbol_list):
        for symbol in symbol_list:
            list_obj = self.trade_client.get_history_orders(symbol=symbol, start_time=None, end_time=None, size=20,
                                                            direct=None)
            LogInfo.output_list(list_obj)

    def get_match_result(self, symbol):
        list_obj = self.trade_client.get_match_result(symbol=symbol, size=5)
        LogInfo.output_list(list_obj)

    def get_open_orders(self, id, symbol):
        list_obj = self.trade_client.get_open_orders(symbol=symbol, account_id=id, direct=QueryDirection.NEXT)
        LogInfo.output_list(list_obj)
        # list_obj = self.trade_client.get_open_orders(symbol=symbol, account_id=id, direct=QueryDirection.PREV)
        # LogInfo.output_list(list_obj)

    def get_order(self, order_id):
        order_obj = self.trade_client.get_order(order_id=order_id)
        LogInfo.output("======= get order by order id : {order_id} =======".format(order_id=order_id))
        order_obj.print_object()

    def batch_cancel(self, account_id):
        # cancel all the open orders under account
        result = self.trade_client.cancel_open_orders(account_id=account_id)

    def create_order_limit(self, symbol, account_id, order_type, amount, price):
        order_id = self.trade_client.create_order(symbol=symbol, account_id=account_id,
                                                  order_type=order_type,
                                                  source=OrderSource.API, amount=amount, price=price)
        LogInfo.output("created order id : {id}".format(id=order_id))

    def create_order_market(self,symbol, account_id, order_type, value):
        order_id = self.trade_client.create_order(symbol=symbol, account_id=account_id,
                                                  order_type=order_type,
                                                  source=OrderSource.API, amount=value, price=None)
        LogInfo.output("created order id : {id}".format(id=order_id))

    def cancel_order(self, symbol, order_id):
        canceled_order_id = self.trade_client.cancel_order(symbol, order_id)
        if canceled_order_id == order_id:
            LogInfo.output("cancel order {id} done".format(id=canceled_order_id))
        else:
            LogInfo.output("cancel order {id} fail".format(id=canceled_order_id))