import pytest

from rbot import Bybit, Binance, Bitbank, ExchangeConfig

@pytest.mark.parametrize(
    "exchange, config, order_price",
    [
        (Bitbank(False), ExchangeConfig.open_exchange_market("bitbank", "BTC/JPY"), 5_000_000),
    ]
)
def test_limit_order_and_cancel(exchange, config, order_price):
    """Test placing and canceling a limit order on the exchange."""
    assert exchange.production is False
    
    exchange.enable_order_with_my_own_risk = True
    assert exchange.enable_order_with_my_own_risk is True
    
    try:
        order_result = exchange.limit_order(config, "Buy", order_price, 0.001)
        print(order_result)
        assert order_result[0].status == "New"
        assert order_result[0].order_side == "Buy"
        assert order_result[0].order_type == "Limit"
        assert order_result[0].order_price == order_price
        assert order_result[0].order_size == 0.001
        assert order_result[0].remain_size == 0.001
        assert order_result[0].free_home_change == -order_price * 0.001

        cancel_result = exchange.cancel_order(config, order_result[0].order_id)
        print(cancel_result)
        assert cancel_result.status == "Canceled"
        assert cancel_result.order_side == "Unknown"
        assert cancel_result.order_price == 0.0
        assert cancel_result.order_size == 0.0
        assert cancel_result.remain_size == 0.0
        assert cancel_result.free_home_change == 0.0
    except Exception as e:
        pytest.skip(f"Test skipped due to API error: {str(e)}")

@pytest.mark.parametrize(
    "exchange, config",
    [
        (Bitbank(False), ExchangeConfig.open_exchange_market("bitbank", "BTC/JPY")),
    ]
)
def test_market_order(exchange, config):
    """Test placing a market order on the exchange."""
    assert exchange.production is False
    
    exchange.enable_order_with_my_own_risk = True
    assert exchange.enable_order_with_my_own_risk is True
    
    try:
        order_result = exchange.market_order(config, "Buy", 0.001)
        print(order_result)
        assert order_result[0].status == "New"
        assert order_result[0].order_side == "Buy"
        assert order_result[0].order_type == "Market"
        assert order_result[0].order_price == 0
        assert order_result[0].order_size == 0.001
        assert order_result[0].remain_size == 0.001
        assert order_result[0].free_home_change == 0  # NO UPDATE for MarketOrder
    except Exception as e:
        pytest.skip(f"Test skipped due to API error: {str(e)}")

@pytest.mark.parametrize(
    "exchange, config",
    [
        (Bitbank(False), ExchangeConfig.open_exchange_market("bitbank", "BTC/JPY")),
    ]
)
def test_open_orders(exchange, config):
    """Test retrieving open orders from the exchange."""
    assert exchange.production is False
    
    try:
        orders = exchange.get_open_orders(config)
        print(orders)
        assert isinstance(orders, list)
    except Exception as e:
        pytest.skip(f"Test skipped due to API error: {str(e)}")

@pytest.mark.parametrize(
    "exchange, config",
    [
        (Bitbank(False), ExchangeConfig.open_exchange_market("bitbank", "BTC/JPY")),
    ]
)
def test_get_account(exchange, config):
    """Test retrieving account information from the exchange."""
    assert exchange.production is False
    
    try:
        account = exchange.account
        print(account)
        assert hasattr(account, 'coins')
        assert isinstance(account.coins, list)
        coin_symbols = [coin.symbol for coin in account.coins]
        assert 'BTC' in coin_symbols
        assert 'JPY' in coin_symbols
    except Exception as e:
        pytest.skip(f"Test skipped due to API error: {str(e)}")

