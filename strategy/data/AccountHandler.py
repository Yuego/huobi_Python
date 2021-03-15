from huobi.client.account import AccountClient, PrintBasic
from huobi.client.margin import MarginClient
import pandas as pd
from TradeHandler import TradeHandler
from huobi.constant import *
from huobi.utils import *
import sqlite3


class AccountHandler:
    def __init__(self):
        self.api_key = "db8f342a-3540b528-hrf5gdfghe-5e793"
        self.secret_key = "366684d9-94c5fdf7-ad3b02a0-446bf"
        self.account_client = AccountClient(api_key=self.api_key, secret_key=self.secret_key)
        self.margin_client = MarginClient(api_key=self.api_key, secret_key=self.secret_key)
        self.accounts = []
        self.asset = {}
        self.balance = pd.DataFrame(columns=['account', 'currency', 'type', 'balance'])
        self.margin = {}
        self.th = TradeHandler()
        self.conn = sqlite3.connect('market.db', timeout=10)
        self.open_order = 0

    def get_accounts(self):
        self.accounts = self.account_client.get_accounts()
        # LogInfo.output_list(self.accounts)

    def get_account_asset(self, account_type, currency):
        asset_valuation = self.account_client.get_account_asset_valuation(account_type=account_type,
                                                                          valuation_currency=currency)
        return float(asset_valuation.balance)

    def update_asset(self):
        for item in self.accounts:
            self.asset[item.type] = self.get_account_asset(item.type, 'usd')

    def get_balance(self, account_id):
        list_obj = self.account_client.get_balance(account_id=account_id)
        for item in list_obj:
            if float(item.balance) != 0:
                self.balance = self.balance.append(
                    pd.Series([account_id, item.currency, item.type, float(item.balance)],
                              index=['account', 'currency', 'type', 'balance']),
                    ignore_index=True)
        self.balance = self.balance.drop_duplicates()

    def update_balance(self):
        self.balance = pd.DataFrame(columns=['account', 'currency', 'type', 'balance'])
        for item in self.accounts:
            self.get_balance(item.id)
        self.balance = self.balance.loc[self.balance['balance'] != 0].reset_index(drop=True)

    def get_cross_margin_account(self):
        account_balance = self.margin_client.get_cross_margin_account_balance()
        self.margin['balance'] = float(account_balance.acct_balance_sum)
        self.margin['debt'] = float(account_balance.debt_balance_sum)
        # account_balance.print_object()

    def get_account_ledger(self, account_id):
        list_obj = self.account_client.get_account_ledger(account_id=account_id)
        LogInfo.output_list(list_obj)

    def update_history_trades(self, symbol):
        table = symbol + "_trade_log"
        self.th.get_match_result(symbol)
        data = self.th.trade_log.copy()
        c = self.conn.cursor()
        # get the count of tables with the name
        c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" + table + "'")
        # #if the count is 1, then table exists
        if c.fetchone()[0] == 1:
            last_data = pd.read_sql('select * from ' + table, self.conn)
            last_data = last_data.append(data).drop_duplicates().sort_values(by='Time', ascending=True)
            last_data.sort_values(by='Time', ascending=True).to_sql(table, self.conn, if_exists='replace', index=False)
        else:
            data.sort_values(by='Time', ascending=True).to_sql(table, self.conn, if_exists='replace', index=False)

    def adjust_position(self, account, currency, target, last_price):
        balance = self.balance
        current_balance = balance[(balance['account'] == account) & (balance['currency'] == currency)]
        if len(current_balance):
            current_position = current_balance['balance'].values[0]
        else:
            current_position = 0.0

        if current_position < target * 0.95:
            od = OrderType.BUY_MARKET
            amt = (target - current_position) * last_price
            if amt < 5:
                return
            else:
                self.place_market_order(currency, account, od, amt)
        elif current_position > target * 1.05 + 0.00005:
            amt = round(current_position - target - 0.00001, 5)
            od = OrderType.SELL_MARKET
            if amt < 0.0001:
                return
            else:
                self.place_market_order(currency, account, od, amt)

    def place_market_order(self, currency, account, od, amt):
        try:
            self.th.batch_cancel(account)
            order_id = self.th.create_order_market(currency + "usdt", account, od, round(amt, 6))
            self.open_order = order_id
        except Exception as error:
            print(error)
            print(od)
            print(amt)
            print("CANNOT PLACE ORDER!!!!!")

    def check_latest_order(self):
        if self.open_order > 0:
            return self.th.get_order(self.open_order)

# ac = AccountHandler()
# ac.get_accounts()
# ac.update_asset()
# ac.update_balance()
# ac.get_cross_margin_account()
