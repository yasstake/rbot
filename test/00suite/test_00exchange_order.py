import pytest

from rbot import Bybit, BybitConfig

def test_limit_order():
    exchange = Bybit(False)
    config = BybitConfig.BTCUSDT
    assert(exchange.production == False)
    
    exchange.enable_order_with_my_own_risk = True
    assert(exchange.enable_order_with_my_own_risk == True)
    print(exchange)
    
    result = exchange.limit_order(config, "Buy", 50_000, 0.001)

    print(result)
    
    
