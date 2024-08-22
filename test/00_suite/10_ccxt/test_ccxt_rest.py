
import ccxt

class CCXTApi:
    def __init__(self, exchange_name, production):
        self.exchange = getattr(ccxt, exchange_name)()
        self.exchange.load_markets()
        
        if not production:
            self.exchange.set_sandbox_mode(True)
            
    def get_board_snapshot(self):
        pass
    
    def get_trades(self, symbol):
        trade = self.exchange.fetch_trades(symbol, params={})
        pass
    
    def get_recent_trades(self):
        pass
    
    def get_klines(self):
        pass
    

import pytest


def test_get_trades():
    api = CCXTApi('binance', True)

    api.get_trades('BTC/USDT')
    
    