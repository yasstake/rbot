
from cryptofeed import FeedHandler

from cryptofeed import *

from cryptofeed.defines import TRADES, TICKER
#from cryptofeed import trade
from cryptofeed.exchanges import Bybit

fh = FeedHandler()

async def trade(t, receipt_timestamp):
    print(f"Trade received at {receipt_timestamp}: {t}")

trade_cb = {TRADES: trade}

fh.add_feed(Bybit(symbols=['BTC-USDT'], channels=[TRADES], callbacks=trade_cb))

fh.run()


