from huobi.client.account import AccountClient
from huobi.constant import *

account_client = AccountClient(api_key="db8f342a-3540b528-hrf5gdfghe-5e793",
                               secret_key="366684d9-94c5fdf7-ad3b02a0-446bf")

account_type = "spot"
asset_valuation = account_client.get_account_asset_valuation(account_type=account_type, valuation_currency="usd")
asset_valuation.print_object()
