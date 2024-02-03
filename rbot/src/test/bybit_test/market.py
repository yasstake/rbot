from rbot import Bybit
from rbot import BybitConfig, BybitMarket
from rbot import init_debug_log, init_log
from rbot.rest import register, start

bybit = Bybit(testnet=True)

market = bybit.open_market(BybitConfig.BTCUSDT)



#market.start_market_stream()
#market.download(ndays=1, verbose=True)

#market.start_user_stream()
init_log()

market.broadcast_message = True
market.start_market_stream()
market.start_user_stream()

register(market)
start(port=5000)
