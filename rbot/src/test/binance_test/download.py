from rbot import BinanceConfig, BinanceMarket
from time import sleep
from rbot import init_log, init_debug_log


config = BinanceConfig.BTCUSDT
config.db_base_dir="./"

#init_debug_log()
market = BinanceMarket(config)


#market.download(ndays=100, verbose=True)
print("download 1")

market.download(ndays=1, verbose=True)

print("download 2")

if market.is_db_thread_running():
    print("1:db thread is running")
else:
    print("1:db thread is not running")

sleep(4)

market.download(ndays=1, verbose=True)

if market.is_db_thread_running():
    print("2:db thread is running")
else:
    print("2:db thread is not running")


market.stop_db_thread()

if market.is_db_thread_running():
    print("3:db thread is running")
else:
    print("3:db thread is not running")

sleep(3)

