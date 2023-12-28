from rbot import Bybit
from rbot import BybitConfig, BybitMarket
from rbot import init_debug_log

bybit = Bybit()

market = bybit.open_market(BybitConfig.SPOT_BTCUSDT)

#init_debug_log()

#market.download_latest(verbose=True)
market.start_market_stream()

market.download(ndays=1, verbose=True)

#market.start_market_stream()

from time import sleep
sleep(10)
