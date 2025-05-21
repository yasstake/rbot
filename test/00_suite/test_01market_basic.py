import pytest

from rbot import Bybit, BybitConfig, Binance, BinanceConfig, init_debug_log

from time import sleep


@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(False), "BTC/USDT"),
            (Binance(False), "BTC/USDC"),
            (Binance(False), "BTC/USDT"),
            (Binance(False), "BTC/USDT:USDT"),
    ]
)
def test_call_select_methods(exchange, config):
    init_debug_log()
    
    market = exchange.open_market(config)
    
    market.download(ndays=3, verbose=True)
    
    sleep(1)
    
    print("market = ", market)
    
    print("start_time / end_time")
    print("start_time = ", market.start_time)
    print(" end_time  = ", market.end_time)
    
    print("archive_info = ", market.archive_info)
    print("db_info      = ", market.db_info)
    
    print("_select_db_trades", market._select_db_trades(0, 0))
    print("_select_archive_trades", market._select_archive_trades(0, 0))
    
    
@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(True), "BTC/USDT"),
            (Bybit(True), "BTC/USDT:USDT"),
            (Binance(True), "BTC/USDT"),
            (Binance(True), "BTC/USDC"),
    ]
)
def test_open_market_stream(exchange, config):
    market = exchange.open_market(config)
    
    init_debug_log()
    
    market.open_market_stream()
    sleep(5)    
