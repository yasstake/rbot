from rbot import Bybit
from rbot import BybitConfig, BybitMarket
from rbot import init_debug_log

bybit = Bybit()

market = bybit.open_market(BybitConfig.SPOT_BTCUSDT)

init_debug_log()

market.start_market_stream()

from time import sleep
sleep(300)
