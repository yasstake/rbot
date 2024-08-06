
from rbot import Bybit, BybitConfig

def test_limit_order_and_cancel():
    exchange = Bybit(False)
    config = BybitConfig.BTCUSDT
    assert(exchange.production == False)
    
    exchange.enable_order_with_my_own_risk = True
    assert(exchange.enable_order_with_my_own_risk == True)
    
    order_result = exchange.limit_order(config, "Buy", 50_000, 0.001)
    print(order_result)
    assert(order_result[0].status == "New")
    assert(order_result[0].order_side == "Buy")
    assert(order_result[0].order_type == "Limit")
    assert(order_result[0].order_price == 50_000)
    assert(order_result[0].order_size == 0.001)
    assert(order_result[0].remain_size == 0.001)
    assert(order_result[0].free_home_change == -50_000 * 0.001)

    cancel_result = exchange.cancel_order(config, order_result[0].order_id)
    print(cancel_result)
    assert(cancel_result.status == "Canceled")
    assert(cancel_result.order_side == "Unknown")
    assert(cancel_result.order_price == 0.0)
    assert(cancel_result.order_size == 0.0)
    assert(cancel_result.remain_size == 0.0)
    assert(cancel_result.free_home_change == 0.0)
   
    

def test_market_order():
    exchange = Bybit(False)
    config = BybitConfig.BTCUSDT
    assert(exchange.production == False)
    
    exchange.enable_order_with_my_own_risk = True
    assert(exchange.enable_order_with_my_own_risk == True)
    
    order_result = exchange.market_order(config, "Buy", 0.001)
    print(order_result)
    assert(order_result[0].status == "New")
    assert(order_result[0].order_side == "Buy")
    assert(order_result[0].order_type == "Market")
    assert(order_result[0].order_price == 0)
    assert(order_result[0].order_size == 0.001)
    assert(order_result[0].remain_size == 0.001)
    assert(order_result[0].free_home_change == 0)     # NO UPDATE for MarketOrder


def test_open_orders():
    exchange = Bybit(False)
    config = BybitConfig.BTCUSDT
    assert(exchange.production == False)
    
    orders = exchange.get_open_orders(config)
    print(orders)            
    
def test_get_account():
    exchange = Bybit(False)
    config = BybitConfig.BTCUSDT
    assert(exchange.production == False)
    
    account = exchange.account
    print(account)            
   
