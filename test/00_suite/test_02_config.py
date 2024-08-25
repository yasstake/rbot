import pytest

from rbot import ExchangeConfig

def test_list_exchange():
    print(ExchangeConfig.exchanges)
    pass


    exchange = ExchangeConfig.open('bybit')

@pytest.mark.parametrize(
    "exchange, production",
    [
            ('bybit', True),
            ('binance', True)
    ]
)
def test_open_exchange(exchange, production):
    exchange = ExchangeConfig.open(exchange)
    print(exchange)    


def test_list_symbols():
    exchange = ExchangeConfig.open('bybit')
    
    print(exchange)
    
    print(exchange.symbols)
    
    market = exchange.open_market("BTC/USDT")