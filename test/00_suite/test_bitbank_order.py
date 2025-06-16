import pytest
from rbot import Bitbank, BitbankConfig

def test_limit_order_and_cancel():
    """Test placing and canceling a limit order on Bitbank.
    
    This test verifies that:
    1. A limit order can be placed successfully
    2. The order details are correct (price, size, status)
    3. The order can be canceled
    4. The cancel response has the expected format
    """
    exchange = Bitbank(False)
    config = BitbankConfig.BTCJPY
    assert(exchange.production == False)
    
    exchange.enable_order_with_my_own_risk = True
    assert(exchange.enable_order_with_my_own_risk == True)
    
    order_result = exchange.limit_order(config, "Buy", 5_000_000, 0.001)
    print(order_result)
    assert(order_result[0].status == "New")
    assert(order_result[0].order_side == "Buy")
    assert(order_result[0].order_type == "Limit")
    assert(order_result[0].order_price == 5_000_000)
    assert(order_result[0].order_size == 0.001)
    assert(order_result[0].remain_size == 0.001)
    assert(order_result[0].free_home_change == -5_000_000 * 0.001)
    assert(order_result[0].category == "spot")  # Bitbank specific
    assert(order_result[0].symbol == "BTC/JPY")  # Bitbank specific

    cancel_result = exchange.cancel_order(config, order_result[0].order_id)
    print(cancel_result)
    assert(cancel_result.status == "Canceled")
    assert(cancel_result.order_side == "Unknown")
    assert(cancel_result.order_price == 0.0)
    assert(cancel_result.order_size == 0.0)
    assert(cancel_result.remain_size == 0.0)
    assert(cancel_result.free_home_change == 0.0)

def test_market_order():
    """Test placing a market order on Bitbank.
    
    This test verifies that:
    1. A market order can be placed successfully
    2. The order details are correct (size, status)
    3. The order type is correctly set to Market
    4. The free_home_change is not updated for market orders
    """
    exchange = Bitbank(False)
    config = BitbankConfig.BTCJPY
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
    assert(order_result[0].category == "spot")  # Bitbank specific
    assert(order_result[0].symbol == "BTC/JPY")  # Bitbank specific

def test_open_orders():
    """Test retrieving open orders from Bitbank.
    
    This test verifies that:
    1. The open orders endpoint is accessible
    2. The response can be parsed successfully
    """
    exchange = Bitbank(False)
    config = BitbankConfig.BTCJPY
    assert(exchange.production == False)
    
    orders = exchange.get_open_orders(config)
    print(orders)
    # Verify the response is a list
    assert(isinstance(orders, list))
    # If there are orders, verify their structure
    if orders:
        assert(orders[0].category == "spot")
        assert(orders[0].symbol == "BTC/JPY")

def test_get_account():
    """Test retrieving account information from Bitbank.
    
    This test verifies that:
    1. The account endpoint is accessible
    2. The response can be parsed successfully
    3. The account contains the expected coin information
    """
    exchange = Bitbank(False)
    config = BitbankConfig.BTCJPY
    assert(exchange.production == False)
    
    account = exchange.account
    print(account)
    # Verify the account has coins
    assert(hasattr(account, 'coins'))
    assert(isinstance(account.coins, list))
    # Verify BTC and JPY are in the coins list
    coin_symbols = [coin.symbol for coin in account.coins]
    assert('BTC' in coin_symbols)
    assert('JPY' in coin_symbols) 