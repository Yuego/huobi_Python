from huobi.client.account import AccountClient
from huobi.client.margin import MarginClient
from huobi.constant import *

# get accounts
from huobi.utils import *

account_client = AccountClient(api_key='db8f342a-3540b528-hrf5gdfghe-5e793',
                              secret_key='366684d9-94c5fdf7-ad3b02a0-446bf')
def callback(account_balance_req: 'AccountBalanceReq'):
    account_balance_req.print_object()

account_client.req_account_balance(callback=callback, client_req_id=None)