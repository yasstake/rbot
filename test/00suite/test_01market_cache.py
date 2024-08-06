import pytest

from rbot import Bybit, BybitConfig


def test_download_cache_initial():
    exchange = Bybit(production=False)
    config = BybitConfig.BTCUSDT
    market = exchange.open_market(config)

    # 最初はキャッシュされない。
    cache = market._select_cache_df(0, 0)
    assert(cache.shape[0] == 0)
    
    market.download(
        ndays=1,        # specify from past days
        force=True,    # if false, the cache data will be used.
        verbose=False    # verbose to print download progress.
    )
    
    # データダウンロード後もキャッシュされない。
    cache = market._select_cache_df(0, 0)
    assert(cache.shape[0] == 0)
    
    trade_df = market.select_trades(0, 0)
    print(trade_df)

    # OHLCを計算するとキャッシュされる。
    ohlcv = market.ohlcv(0, 0, 10)
    
    cache = market._select_cache_df(0, 0)
    print("## cache len=", cache.shape[0])
    
    print(cache)
    assert(trade_df.shape[0] == cache.shape[0])
    
    assert(trade_df['size'].sum() == cache['size'].sum())
    assert(abs(ohlcv['volume'].sum() - cache['size'].sum()) < 0.01)

from time import sleep

def test_download_cache_latest_update():
    exchange = Bybit(production=False)
    config = BybitConfig.BTCUSDT
    market = exchange.open_market(config)

    # 最初はキャッシュされない。
    cache = market._select_cache_df(0, 0)
    assert(cache.shape[0] == 0)
    
    market.download(
        ndays=1,        # specify from past days
        force=True,    # if false, the cache data will be used.
        verbose=False    # verbose to print download progress.
    )
    
    sleep(1)        # Wait for data settlement
    
    # データダウンロード後もキャッシュされない。
    cache = market._select_cache_df(0, 0)
    assert(cache.shape[0] == 0)
    
    trade_df = market.select_trades(0, 0)
    print(trade_df)

    # OHLCを計算するとキャッシュされる。
    ohlcv = market.ohlcv(0, 0, 10)

    sleep(2)        # Wait for data settlement

    cache = market._select_cache_df(0, 0)

    # キャッシュ後、データをロードするとキャッシュとDBの間に差異が発生する。
    market._download_latest()

    sleep(1)

    # `キャッシュデータは変化しなし`    
    cache2 = market._select_cache_df(0, 0)
    
    print('cache  len = ', cache.shape[0])
    print('cache2 len = ', cache2.shape[0])
    
    assert(cache.shape[0] == cache2.shape[0])
    
    db = market.select_trades(0, 0)
    print("db len = ", db.shape[0])    
    assert(cache2.shape[0] != db.shape[0])
    
    # OHLCを計算するとキャッシュされる。
    ohlcv = market.ohlcv(0, 0, 10)
    cache3 = market._select_cache_df(0, 0)
    assert(cache3.shape[0] != db.shape[0])

