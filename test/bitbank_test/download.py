from rbot import Bitbank
from time import sleep
from rbot import init_log, init_debug_log

#init_debug_log()

bitbank = Bitbank(True)

market = bitbank.open_market("BTC/JPY")

market.download(ndays=10, force=False, verbose=True)

sleep(5)

print(market._repr_html_())