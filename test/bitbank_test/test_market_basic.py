import pytest
from rbot import Bitbank, init_debug_log
from time import sleep


@pytest.mark.parametrize(
    "config",
    [
        "BTC/JPY",
        "ETH/JPY",
        "XRP/JPY",
    ]
)
def test_market_basic(config):
    """Test basic market functionality for Bitbank"""
    init_debug_log()
    
    exchange = Bitbank(False)  # False for non-streaming mode
    market = exchange.open_market(config)
    
    # Test market data download
    market.download(ndays=3, verbose=True)
    sleep(1)
    
    # Print market information
    print(f"Market: {market}")
    print(f"Start time: {market.start_time}")
    print(f"End time: {market.end_time}")
    print(f"Archive info: {market.archive_info}")
    print(f"DB info: {market.db_info}")
    
    # Test data selection methods
    print("DB trades:", market._select_db_trades(0, 0))
    print("Archive trades:", market._select_archive_trades(0, 0))


@pytest.mark.parametrize(
    "config",
    [
        "BTC/JPY",
        "ETH/JPY",
        "XRP/JPY",
    ]
)
def test_market_stream(config):
    """Test market streaming functionality for Bitbank"""
    exchange = Bitbank(True)  # True for streaming mode
    market = exchange.open_market(config)
    
    init_debug_log()
    
    # Test market stream
    market.open_market_stream()
    sleep(5)  # Let it run for 5 seconds to collect some data 