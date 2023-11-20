
from rbot import Session
from rbot import Runner
from rbot import BinanceConfig
from rbot import BinanceMarket
from rbot import init_debug_log
from rbot import init_log
from rbot import OrderSide
from rbot import time_string
from rbot import OrderStatus
from rbot import SEC

MARGIN = 1.0

class MyAgent:
    def __init__(self):
        self.last_update = 0
        self.order_size = 0.001

    def on_clock(self, session, clock):
        print("on_clock", time_string(clock))
        if 0 < len(session.buy_orders):
            if session.current_time + SEC(60) < session.buy_orders[0].create_time:
                session.cancel_order(session.buy_orders[0].order_id)
                print("cancel buy order")
        
        if 0 < len(session.sell_orders):
            if session.current_time + SEC(60) < session.sell_orders[0].create_time:
                session.cancel_order(session.sell_orders[0].order_id)
                print("cancel sell order")
                
            

    def on_tick(self, session, side, price, size):
        if not(session.buy_edge + 0.001 < session.sell_edge):
            return # スプレッドが狭い場合は何もしない

        if session.buy_order_amount == 0:
            session.limit_order(OrderSide.Buy, session.buy_edge - MARGIN, self.order_size)
            print("try make buy order", session.home_change, session.foreign_change)
            
        if session.sell_order_amount == 0:
            session.limit_order(OrderSide.Sell, session.sell_edge + MARGIN, self.order_size)
            print("try make sell order", session.home_change, session.foreign_change)
    
#    def on_update(self, session, updated_order):
#        print("on_update", updated_order)
        
    def on_account_update(self, session, account):
        print("account update: ", session.current_time, account)

    
market = BinanceMarket(BinanceConfig.TEST_BTCUSDT)

print(BinanceConfig.BTCUSDT)

agent = MyAgent()
runner = Runner()

from threading import Thread
from time import sleep


def run():
    session = runner.back_test(market, agent, 60, 0, 0)
    print(session.log)
    print(session.home_change, session.foreign_change, )

run()




    