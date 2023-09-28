
from rbot import Session
from rbot import Runner
from rbot import BinanceConfig
from rbot import BinanceMarket
from rbot import init_debug_log
from rbot import init_log
from rbot import OrderSide
from rbot import time_string

class MyAgent:
    def __init__(self):
        pass
    
    def on_clock(self):
        pass


    def on_tick(self, session, side, price, size):
        print("tick: ", session.current_time, side, price, size)
        
        market.limit_order(OrderSide.Sell, price + 100, 0.001)        
        pass
    
    def on_update(self, session, updated_order):
        print("update: ", time_string(session.current_time), 
              updated_order.order_side,
              updated_order.order_price,
              updated_order.order_size,
              updated_order.execute_size,
              time_string(updated_order.create_time),
              time_string(updated_order.update_time),
              updated_order.status,
              )
        print("buy orders", session.buy_orders)
        print("sell orders", session.sell_orders)
        print("account", session.account)
        print("-------------------")
    
    def on_account_update(self, session, account):
        print("account update: ", session.current_time, account)
        pass
    
market = BinanceMarket(BinanceConfig.TEST_BTCUSDT)

print(BinanceConfig.TEST_BTCUSDT)

market.start_market_stream()
market.start_user_stream()
    
agent = MyAgent()
runner = Runner()

from threading import Thread
from time import sleep


def run():
    runner.run(market, agent)


run()




    