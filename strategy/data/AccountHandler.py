from huobi.client.account import AccountClient, PrintBasic
from huobi.client.margin import MarginClient
import pandas as pd
from huobi.constant import *
from huobi.constant import *
from huobi.constant import *

# get accounts
from huobi.utils import *


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

    def update_balance(self):
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


ac = AccountHandler()
ac.get_accounts()
ac.update_asset()
ac.update_balance()
ac.get_cross_margin_account()
print(ac.margin)
