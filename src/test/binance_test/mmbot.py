
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
            session.limit_order(OrderSide.Sell, ask_edge + 2.5, 0.01)
        else:
            sell_price = session.sell_orders[0].order_price
            if sell_price - ask_edge  > 25.0:
                session.cancel_order(session.sell_orders[0].order_id)
                session.limit_order(OrderSide.Sell, ask_edge + 2.5, 0.01)

        if len(session.buy_orders) == 0:
            session.limit_order(OrderSide.Buy, bid_edge - 2.5, 0.01)
        else:            
            buy_price = session.buy_orders[0].order_price
            if  bid_edge - buy_price > 25.0:
                session.cancel_order(session.buy_orders[0].order_id)
                session.limit_order(OrderSide.Buy, bid_edge - 2.5, 0.01)


    def on_update(self, session, updated_order):
        self.home += updated_order.home_change
        self.foreign += updated_order.foreign_change
        
        total = self.home + self.foreign * session.bids[0]['price'][0]
        
        print("UPDATE_TOTAL", self.home, self.foreign, total)
        print("Order Update: ", time_string(session.current_time), updated_order)
        
    
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
