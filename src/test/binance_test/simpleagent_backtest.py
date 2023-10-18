
from rbot import Session
from rbot import Runner
from rbot import BinanceConfig
from rbot import BinanceMarket
from rbot import init_debug_log
from rbot import init_log
from rbot import OrderSide
from rbot import time_string
from rbot import OrderStatus


class MyAgent:
    def __init__(self):
        self.last_update = 0
        self.order_size = 0.001
        self.has_position = False

    def on_clock(self, session, clock):
        print("on_clock", time_string(clock))

        print("sell edge", session.sell_edge)
        print("buy edge ", session.buy_edge)        
    
        order = session.market_order(OrderSide.Sell, self.order_size)                
        print("sell order", order)

    
        order = session.market_order(OrderSide.Buy, self.order_size)                    
        print("buy order", order)

    
    def on_update(self, session, updated_order):
        print("on_update", updated_order)
        
    
    def on_account_update(self, session, account):
        print("account update: ", session.current_time, account)

    
market = BinanceMarket(BinanceConfig.TEST_BTCUSDT)

print(BinanceConfig.TEST_BTCUSDT)

agent = MyAgent()
runner = Runner()

from threading import Thread
from time import sleep



def run():
    session = runner.back_test(market, agent, 3600, 0, 0)
    print(session.log)

run()




    