import pytest

import rbot

from rbot import Bybit, BybitConfig, time_string, DAYS, NOW, HHMM
from rbot import Binance, BinanceConfig

@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(False), BybitConfig.BTCUSDT),
            (Bybit(False), BybitConfig.BTCUSDC),
            (Binance(False), BinanceConfig.BTCUSDT)
    ]
)
def test_get_config(exchange, config):
    market = exchange.open_market(config)

    c2 = market.config
    print("config = ", c2)
    assert(c2 == config)

@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(False), BybitConfig.BTCUSDT),
            (Bybit(False), BybitConfig.BTCUSDC),
            (Binance(False), BinanceConfig.BTCUSDT)
    ]
)
def test_start_time_and_db_info(exchange, config):
    market = exchange.open_market(config)
    
    market.download(3, verbose=True, force=True)
    
    start_time = market.start_time
    end_time = market.end_time   
    print("total ", time_string(start_time), "/", time_string(end_time))

    ac_start, ac_end = market.archive_info
    print("archive ", time_string(ac_start), "/", time_string(ac_end))

    db_start, db_end = market.db_info
    print("db      ", time_string(db_start), "/", time_string(db_end))

    assert(start_time < end_time)
    assert(start_time + DAYS(1) <= end_time)

    assert(start_time == ac_start)
    assert(end_time == db_end)
    
    gap =  db_start - ac_end
    assert(gap <= 15_000_000)     # ohlcv interval
    
@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(False), BybitConfig.BTCUSDT),
            (Bybit(False), BybitConfig.BTCUSDC),
            (Binance(False), BinanceConfig.BTCUSDT)
    ]
)
def test_df_merged(exchange, config):
    market = exchange.open_market(config)
    
    market.download(3, verbose=True)
  
    total_df = market.select_trades(0, 0)
    min_time = total_df["timestamp"].min()
    end_time = total_df['timestamp'].max()
    count = total_df.shape[0]
    print("total = ", count, min_time, end_time)

    db_df = market._select_db_trades(0, 0) 
    db_start = db_df['timestamp'].min()
    db_end = db_df['timestamp'].max()
    db_count = db_df.shape[0]
    print("db =    ",db_count, db_start, db_end)    

    ac_df = market._select_archive_trades(0, 0)

    ac_start = ac_df['timestamp'].min()
    ac_end = ac_df['timestamp'].max()
    ac_count = ac_df.shape[0]
    print("ac  =   ", ac_count, ac_start, ac_end)
    
    assert(ac_count + db_count == count)
    assert(ac_start == min_time)
    assert(db_end == end_time)
    
@pytest.mark.parametrize(
    "exchange, config",
    [
            (Bybit(False), BybitConfig.BTCUSDT),
            (Bybit(False), BybitConfig.BTCUSDC),
            (Binance(False), BinanceConfig.BTCUSDT)
    ]
)
def test_download_latest(exchange, config):
    market = exchange.open_market(config)
    
    market.download(3, verbose=True)

    now = NOW()
    start = now- HHMM(0, 10)
    end = now - HHMM(0, 1)

    ohlcv = market.ohlcvv(start, end, 1) 
    
    print(ohlcv)
    
    d_start, d_end  = market._download_latest()

    print(market.select_trades(start, end))