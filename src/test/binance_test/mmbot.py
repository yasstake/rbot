
from rbot import Session
from rbot import Runner
from rbot import BinanceConfig
from rbot import BinanceMarket
from rbot import init_debug_log
from rbot import init_log
from rbot import OrderSide
from rbot import time_string
from threading import Thread
from time import sleep

class MyAgent:
    def __init__(self):
        self.home = 0
        self.foreign = 0
    
    def on_clock(self):
        pass
    
    def on_tick(self, session, side, price, size):
        if len(session.asks) == 0 or len(session.bids) == 0:
            return

        ask_edge = session.asks[0]['price'][0]
        bid_edge = session.bids[0]['price'][0]


        if len(session.sell_orders) == 0:
            print(">Sell Order, price: ", ask_edge + 0.5, "size: ", 0.001)
            session.limit_order(OrderSide.Sell, ask_edge + 0.5, 0.001)
        else:
            sell_price = session.sell_orders[0].order_price
            if sell_price - ask_edge  > 5.0:
                print(">Sell Order, change price: ", ask_edge, "size: ", 0.001)
                session.cancel_order(session.sell_orders[0].order_id)
                session.limit_order(OrderSide.Sell, ask_edge, 0.001)

        if len(session.buy_orders) == 0:
            print(">Buy Order, price: ", bid_edge - 0.5, "size: ", 0.001)
            session.limit_order(OrderSide.Buy, bid_edge - 0.5, 0.001)
        else:            
            buy_price = session.buy_orders[0].order_price
            if  bid_edge - buy_price > 5.0:
                print(">Buy Order change, price: ", bid_edge, "size: ", 0.001)
                session.cancel_order(session.buy_orders[0].order_id)
                session.limit_order(OrderSide.Buy, bid_edge, 0.001)
       


    def on_update(self, session, updated_order):
        self.home += updated_order.home_change
        self.foreign += updated_order.foreign_change
        
        total = self.home + self.foreign * session.bids[0]['price'][0]
        
        print("UPDATE_TOTAL", self.home, self.foreign, total)
        
    
    def on_account_update(self, session, account):
        #print("account update: ", session.current_time, account)
        print("TOTAL ASSETS: ", account.home + account.foreign * session.bids[0]['price'][0])

    
market = BinanceMarket(BinanceConfig.TEST_BTCUSDT)

market.cancel_all_orders()

print(BinanceConfig.TEST_BTCUSDT)

market.start_market_stream()
market.start_user_stream()
    
agent = MyAgent()
runner = Runner()




runner.run(market, agent)
