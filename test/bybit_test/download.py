from rbot import Bybit, BybitConfig, Runner

exchange = Bybit(production=True)
config = BybitConfig.BTCUSDT
market = exchange.open_market(config)

market.download_archive(
    ndays=30,        # specify from past days
    force=False,    # if false, the cache data will be used.
    verbose=True    # verbose to print download progress.
)

print("market status: ", market.__str__())


from rbot import init_log, init_debug_log

#init_debug_log()
init_log()
from time import sleep


market.download_realtime(True, True)

for i in range(60):
    sleep(1)
    print(market.select_db_trades(0, 0))
    
sleep(1)
print(market.select_db_trades(0, 0))
sleep(1)
print(market.select_db_trades(0, 0))
sleep(1)
print(market.select_db_trades(0, 0))


print(market.select_db_trades(0, 0))
print(market.ohlcv(0, 0, 1))

sleep(15)

print(market.select_db_trades(0, 0))
print(market.ohlcv(0, 0, 1))

sleep(15)
print(market.select_db_trades(0, 0))
print(market.ohlcv(0, 0, 1))

sleep(1)
print(market.select_db_trades(0, 0))
sleep(1)
print(market.select_db_trades(0, 0))
sleep(1)
print(market.select_db_trades(0, 0))
sleep(1)
print(market.select_db_trades(0, 0))
