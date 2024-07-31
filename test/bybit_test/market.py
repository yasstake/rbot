from rbot import Bybit
from rbot import BybitConfig
from rbot import Runner
from rbot import init_debug_log, init_log

bybit = Bybit(production=False)
market = bybit.open_market(BybitConfig.BTCUSDT)

market.start_market_stream()
market.download_archive(ndays=1, verbose=True)
bybit.start_user_stream()

init_debug_log()

runner = Runner()
runner.start_proxy()


