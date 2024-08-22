import pytest

import rbot

from rbot import Bybit, BybitConfig, time_string, DAYS, NOW, HHMM, Binance, BinanceConfig

from time import sleep

@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(True), BybitConfig.BTCUSDT),
            (Binance(True), BinanceConfig.BTCUSDT),
    ]
)
def test_start_stream(exchange, config):
    market = exchange.open_market(config)

    market.download(3)

    market.open_market_stream()
    
    print(market.board)

    for i in range(0, 4):
        print(i)
        print(market.ohlcvv(NOW() - DAYS(2), 0, 6000))
        db_df = market._select_db_trades(NOW() - DAYS(2), 0)
        print(db_df)
        sleep(1)
        