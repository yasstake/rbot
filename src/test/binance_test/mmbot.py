
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

import polars as pl

ORDER_SIZE = 0.001
SPREAD_TRIGGER = 0.0004
CLOSE_TRIGGER =  0.0003
PRICE_DIFF = 0.01
SKIP_SIZE = 0.025

class MyAgent:
    def __init__(self):
        self.home = 0
        self.foreign = 0

    def calc_spread(self, session, skip_size=SKIP_SIZE):
        if len(session.asks) == 0 or len(session.bids) == 0:
            return 0.0, 0.0, 0.0
        
        bid = session.bids.filter(pl.col('size') > skip_size).head()['price'][0]
        ask = session.asks.filter(pl.col('size') > skip_size).head()['price'][0]
        
        return bid, ask, ask - bid

    """    
    def on_tick(self, session, side, price, size):
        # スプレットを確認
        bid, ask, spread = self.calc_spread(session)
        if bid == 0.0:
            return

        amount_buy = session.buy_order_amount
        amount_sell = session.sell_order_amount

        if amount_buy == 0 and amount_sell == 0:
            # スプレッドが大きい場合
            if spread > price * SPREAD_TRIGGER:
                # まだ指値が両サイドに入っていない場合は、両サイドに指値を入れる。
                session.limit_order(OrderSide.Buy, bid + PRICE_DIFF, ORDER_SIZE)
                session.limit_order(OrderSide.Sell, ask - PRICE_DIFF, ORDER_SIZE)
        elif amount_buy != 0 and amount_sell == 0:  # 買いオーダーだけ残っている場合
                # 買いオーダーの乖離をみて、離れていれば売り
                buy_price = session.buy_orders[0].order_price
                if bid - buy_price > price * CLOSE_TRIGGER:
                    order_size = session.buy_orders[0].remain_size
                    session.cancel_order(session.buy_orders[0].order_id)
                    session.market_order(OrderSide.Buy, order_size)
        elif amount_sell != 0 and amount_buy == 0: #　売りオーダーだけ残っている場合
                # 売りオーダーの乖離をみて離れていれば修正            
                sell_price = session.sell_orders[0].order_price    
                if sell_price - ask > price * CLOSE_TRIGGER:
                    order_size = session.sell_orders[0].remain_size
                    session.cancel_order(session.sell_orders[0].order_id)
                    session.market_order(OrderSide.Sell, order_size)
    """

    def on_update(self, session, updated_order):
        self.home += updated_order.home_change
        self.foreign += updated_order.foreign_change
        
        total = self.home + self.foreign * session.bids[0]['price'][0]
        
        print("UPDATE_TOTAL", self.home, self.foreign, total)
        
        print("UPDATE BUY: ", session.buy_orders)
        print("UPDATE SELL: ", session.sell_orders)
        
    
    def on_account_update(self, session, account):
        #print("account update: ", session.current_time, account)
        print("TOTAL ASSETS: ", account.home + account.foreign * session.bids[0]['price'][0])

    

init_log()
    
market = BinanceMarket(BinanceConfig.TEST_BTCUSDT)

market.cancel_all_orders()



print(BinanceConfig.TEST_BTCUSDT)

market.start_market_stream()
market.start_user_stream()
    
agent = MyAgent()
runner = Runner()

#init_debug_log()


runner.run(market, agent)
