import pytest

from rbot import Bybit, init_debug_log, Binance, Bitbank

@pytest.mark.parametrize(
    "exchange, production",
    [
            (Bybit(False), False),
            (Bybit(True), True),
            (Binance(False), False),
            (Binance(True), True),
            (Bitbank(False), False),
            (Bitbank(True), True)
    ]
)
def test_get_property(exchange, production):
#    exchange = Bybit(False)
    
    # get_production
    assert(exchange.production == production)
    print(exchange)    
    # get_enable_order_with_my_own_risk
    assert(exchange.enable_order_with_my_own_risk == False)
    print("")
    print ("ENABLE ORODER (default) = ",exchange.enable_order_with_my_own_risk)  
    exchange.enable_order_with_my_own_risk = True   
    print ("ENABLE ORODER (update) = ", exchange.enable_order_with_my_own_risk)  

    print(exchange.__str__())
    
    assert(exchange.enable_order_with_my_own_risk == True)
    
