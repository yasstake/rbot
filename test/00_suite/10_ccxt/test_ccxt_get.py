
import pytest
import ccxt
import polars as pl

def test_get_klines():
    exchange = ccxt.binance()
   
    
    ohlcv = exchange.fetch_ohlcv("BTC/USDT", '1m') 
    
    #print(ohlcv)

    ohlcv_df = pl.DataFrame(ohlcv)

    print(ohlcv_df)
    
    