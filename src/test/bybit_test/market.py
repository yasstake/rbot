from rbot import Bybit
from rbot import BybitConfig, BybitMarket
from rbot import init_debug_log

bybit = Bybit(True)

market = bybit.open_market(BybitConfig.BTCUSDT)


init_debug_log()

#market.start_market_stream()
market.download(ndays=1, verbose=True)

market.start_user_stream()

from time import sleep
print("Sleeping 500 seconds")
sleep(500)
