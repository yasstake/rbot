import pytest

from rbot import Bybit, init_debug_log

"""
@pytest.mark.parametrize(
    "exchange, production",
    [
            (Bybit(False), False),
            (Bybit(True), True)
    ]
)
"""
def test_get_property():
    exchange = Bybit(False)
    
    # get_production
    assert(exchange.production == False)
    print(exchange)    
    # get_enable_order_with_my_own_risk
    assert(exchange.enable_order_with_my_own_risk == False)
    print("")
    print ("ENABLE ORODER (default) = ",exchange.enable_order_with_my_own_risk)  
    exchange.enable_order_with_my_own_risk = True   
    print ("ENABLE ORODER (update) = ", exchange.enable_order_with_my_own_risk)  

    print(exchange.__str__())
    
    assert(exchange.enable_order_with_my_own_risk == True)
    
