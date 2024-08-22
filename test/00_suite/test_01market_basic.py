import pytest

from rbot import Bybit, BybitConfig, Binance, BinanceConfig


@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(False), BybitConfig.BTCUSDT),
            (Binance(False), BinanceConfig.BTCUSDT),
    ]
)
def test_call_all_methods(exchange, config):
    market = exchange.open_market(config)
    
    print("market = ", market)
    
    c2 = market.config
    assert(config == c2)

    print("start_time / end_time")
    print("start_time = ", market.start_time)
    print(" end_time  = ", market.end_time)
    
    print("archive_info = ", market.archive_info)
    print("db_info      = ", market.db_info)

    print("_select_db_trades", market._select_db_trades(0, 0))
    print("_select_archive_trades", market._select_archive_trades(0, 0))
    