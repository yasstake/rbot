import pytest

from rbot import ExchangeConfig

def test_list_exchange():
    exchanges = ExchangeConfig.exchanges
    assert exchanges is not None
    assert isinstance(exchanges, (list, tuple))
    assert len(exchanges) > 0
    assert 'bybit' in exchanges
    assert 'binance' in exchanges

itewriwrite
def test_open_exchange_basic():
    exchange = ExchangeConfig.open('bybit')
    assert exchange is not None
    assert hasattr(exchange, 'symbols')


@pytest.mark.parametrize(
    "exchange, production",
    [
        ('bybit', True),
        ('binance', True),
        ('bybit', False),
        ('binance', False)
    ]
)
def test_open_exchange(exchange, production):
    exchange_config = ExchangeConfig.open(exchange, production=production)
    assert exchange_config is not None
    assert hasattr(exchange_config, 'symbols')


def test_open_exchange_invalid():
    with pytest.raises((ValueError, KeyError, AttributeError)):
        ExchangeConfig.open('invalid_exchange')


def test_open_exchange_empty_string():
    with pytest.raises((ValueError, KeyError, AttributeError)):
        ExchangeConfig.open('')


def test_open_exchange_none():
    with pytest.raises((TypeError, ValueError)):
        ExchangeConfig.open(None)


def test_list_symbols():
    exchange = ExchangeConfig.open('bybit')
    assert exchange is not None
    
    symbols = exchange.symbols
    assert symbols is not None
    assert isinstance(symbols, (list, tuple))
    assert len(symbols) > 0
    assert 'BTC/USDT' in symbols


def test_open_market_valid():
    exchange = ExchangeConfig.open('bybit')
    market = exchange.open_market("BTC/USDT")
    assert market is not None


def test_open_market_invalid_symbol():
    exchange = ExchangeConfig.open('bybit')
    with pytest.raises((ValueError, KeyError, AttributeError)):
        exchange.open_market("INVALID/SYMBOL")


def test_open_market_empty_symbol():
    exchange = ExchangeConfig.open('bybit')
    with pytest.raises((ValueError, KeyError, AttributeError)):
        exchange.open_market("")


def test_open_market_none_symbol():
    exchange = ExchangeConfig.open('bybit')
    with pytest.raises((TypeError, ValueError)):
        exchange.open_market(None)


@pytest.mark.parametrize(
    "exchange_name, symbol",
    [
        ('bybit', 'BTC/USDT'),
        ('binance', 'BTC/USDT'),
        ('bybit', 'ETH/USDT'),
        ('binance', 'ETH/USDT')
    ]
)
def test_open_market_multiple_exchanges_symbols(exchange_name, symbol):
    exchange = ExchangeConfig.open(exchange_name)
    market = exchange.open_market(symbol)
    assert market is not None


def test_exchange_config_consistency():
    """Test that opening the same exchange multiple times returns consistent results"""
    exchange1 = ExchangeConfig.open('bybit')
    exchange2 = ExchangeConfig.open('bybit')
    
    assert exchange1.symbols == exchange2.symbols


def test_symbols_contain_common_pairs():
    """Test that exchanges contain expected common trading pairs"""
    exchange = ExchangeConfig.open('bybit')
    symbols = exchange.symbols
    
    common_pairs = ['BTC/USDT', 'ETH/USDT']
    for pair in common_pairs:
        assert pair in symbols, f"Expected {pair} to be in available symbols"


def test_exchange_attributes():
    """Test that exchange objects have expected attributes"""
    exchange = ExchangeConfig.open('bybit')
    
    required_attributes = ['symbols', 'open_market']
    for attr in required_attributes:
        assert hasattr(exchange, attr), f"Exchange should have {attr} attribute"