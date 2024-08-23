import pytest

from rbot import ExchangeConfig

def test_list_exchange():
    print(ExchangeConfig.exchanges)
    pass

def test_list_symbols():
    exchange = ExchangeConfig.open('bybit')
    
    print(exchange)
    
    print(exchange.symbols)
    
    market = exchange.open_market("BTC/USDT")