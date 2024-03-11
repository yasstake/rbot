from rbot import BinanceConfig, Binance
from time import sleep
from rbot import init_log, init_debug_log

#init_debug_log()

config = BinanceConfig.BTCUSDT
binance = Binance(True)


market = binance.open_market(config)

#market.expire_unfix_data(True)

#market.start_market_stream()
#market.download_archive(ndays=2, verbose=True)

#init_debug_log()
#market.download_latest(verbose=True)
market.download_gap(force=False, verbose=True)

sleep(60)