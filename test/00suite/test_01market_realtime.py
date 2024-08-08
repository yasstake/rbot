import pytest

import rbot

from rbot import Bybit, BybitConfig, time_string, DAYS, NOW, HHMM

from time import sleep

def test_start_stream():
    exchange = Bybit(True)
    config = BybitConfig.BTCUSDT
    market = exchange.open_market(config)

    market.download(3)

    market.open_market_stream()
    
    print(market.board)

    for i in range(0, 10):
        print(i)
        print(market.ohlcvv(NOW() - DAYS(2), 0, 6000))
        db_df = market._select_db_trades(NOW() - DAYS(2), 0)
        print(db_df)
        sleep(2)
        