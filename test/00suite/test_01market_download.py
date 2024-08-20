import pytest

from rbot import NOW, HHMM
from rbot import Bybit, BybitConfig, Binance, BinanceConfig


@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(False), BybitConfig.BTCUSDT),
            (Binance(False), BinanceConfig.BTCUSDT),
    ]
)
def test_download_latest(exchange, config):
    market = exchange.open_market(config)

    count = market._download_latest()
    print(count)

from rbot import init_debug_log

@pytest.mark.parametrize(
    "exchange, config",
    [
            #(Bybit(False), BybitConfig.BTCUSDT),
            (Binance(False), BinanceConfig.BTCUSDT),
    ]
)
def test_download_range(exchange, config):
    market = exchange.open_market(config)

    #init_debug_log()

    market._download_range(
        NOW() - HHMM(0, 30),
        0,
        True
    )

from rbot import init_log

@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(False), BybitConfig.BTCUSDT),
            (Binance(False), BinanceConfig.BTCUSDT),
    ]
)
def test_download_archive(exchange, config):
    market = exchange.open_market(config)

    init_debug_log()

    market._download_archive(ndays=5, verbose=True)
    
    print(market._select_archive_trades(0, 0))

    
@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(False), BybitConfig.BTCUSDT),
            (Binance(False), BinanceConfig.BTCUSDT),
    ]
)
def test_download(exchange, config):
    market = exchange.open_market(config)

    market.download(3, force=True)    
    