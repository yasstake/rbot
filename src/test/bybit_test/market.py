from rbot import Bybit
from rbot import BybitConfig, BybitMarket
from rbot import init_debug_log
from rbot.rest import register, start

bybit = Bybit()

market = bybit.open_market(BybitConfig.BTCUSDT)



#market.start_market_stream()
#market.download(ndays=1, verbose=True)

#market.start_user_stream()
init_debug_log()

market.start_market_stream()

register(market)
start(port=5000)
