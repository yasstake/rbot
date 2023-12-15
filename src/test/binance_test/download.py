from rbot import BinanceConfig, BinanceMarket
from time import sleep
from rbot import init_log, init_debug_log


config = BinanceConfig.BTCUSDT
config.db_base_dir="./"

init_log()
market = BinanceMarket(config)


market.download(ndays=100, verbose=True)

print('Waiting for download to finish...')

i= 1
while i < 10:
    if market.inprogress:
        print("Downloading...")
        sleep(5)
    else:
        print("Done!")
        break

    i += 1

#maket.vacuum()

sleep(10)
