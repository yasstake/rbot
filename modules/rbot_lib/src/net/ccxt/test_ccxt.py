
import unittest
import os

from ccxt_api import CCXTApi

class CcxtText(unittest.TestCase):
    def test_board_snapshot(self):
        key, secret = self.get_keys()
        api = CCXTApi('bybit', key, secret)

        board = api.get_board_snapshot("BTC/USDT")
        #print(board)
    
    
    def test_recent_trades(self):
        api = CCXTApi('bybit')

        trades = api.get_recent_trades("BTC/USDT")
        print(trades)
        
    def test_trades(self):
        api = CCXTApi('bybit')

        trades = api.get_trades("BTC/USDT", 0, 0)
        #print(trades)

    def test_klines(self):
        api = CCXTApi('bybit')

        trades = api.get_klines("BTC/USDT", 0, 0)
        #print(trades)

    def test_new_order(self):
        key, secret = self.get_keys()
        api = CCXTApi('bybit', key, secret, production=False)
        
        order = api.new_order('BTC/USDT', 'Limit', 'Buy', 45000, 0.001)
        
        #print(order)

    def test_open_order(self):
        key, secret = self.get_keys()
        api = CCXTApi('bybit', key, secret, production=False)
        
        orders = api.open_orders('BTC/USDT')
        
        #print(orders)

    def test_get_account(self):
        key, secret = self.get_keys()
        api = CCXTApi('bybit', key, secret, production=False)
        
        account = api.get_account()
        
        #print(account)

    def get_keys(self):
        env_vars = os.environ
        
        return env_vars['BYBIT_API_KEY'], env_vars['BYBIT_API_SECRET']
        

if __name__ == "__main__":
    unittest.main()
    #print(exec("printenv"))








