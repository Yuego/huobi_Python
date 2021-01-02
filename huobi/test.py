from client import *


# Create generic client instance and get the timestamp
generic_client = GenericClient()
ts = generic_client.get_exchange_timestamp()
print(timestamp)

# Create the market client instance and get the latest btcusdt‘s candlestick
market_client = MarketClient()
list_obj = market_client.get_candlestick("btcusdt", CandlestickInterval.MIN5, 10)
LogInfo.output_list(list_obj)