
from rbot import Session
from rbot import Runner
from rbot import BinanceConfig
from rbot import BinanceMarket
from rbot import init_debug_log
from rbot import init_log
from rbot import OrderSide
from rbot import time_string
from rbot import HHMM
from rbot import SEC
from rbot import OrderStatus

OFFSET = 0.0

class MyAgent:
    def __init__(self):
        self.last_update = 0
        self.order_size = 0.001
        self.has_position = False

    def on_tick(self, session, side, price, size):
        # 3秒毎に実行
        if session.current_time < self.last_update + SEC(3):
            return        

        self.last_update = session.current_time
        
        # 3秒毎にOHLCを取得。15秒前から5本分
        ohlc = session.ohlcv(3, 5)
        if ohlc.shape[0] < 1:
            return

        close_price = ohlc.tail(1)['close'][0]  # 最新の終値

        # ポジションがある場合は、売りの指値を入れる
        if self.has_position:
            if len(session.sell_orders) == 0: # 売りオーダーがない場合は、売りの指値を入れる。
                session.limit_order(OrderSide.Sell, close_price * (1 + OFFSET), self.order_size)
            else:
                # 1分以上経過している売りオーダーはキャンセル。オーダは次サイクルで実施。
                last_order_time = session.sell_orders[0].create_time
                if last_order_time + HHMM(0, 1) < session.current_time:
                    session.cancel_order(session.sell_orders[0].order_id)
        else:
            if len(session.buy_orders) == 0: # ポジションもなく、オーダーもキューされていない場合は買いの指値を入れる
                session.limit_order(OrderSide.Buy, close_price * (1 - OFFSET), self.order_size)    
            else:
                # 1分以上経過している買いオーダーは、キャンセル。オーダは次サイクルで実施。                                
                last_order_time = session.buy_orders[0].create_time
                if last_order_time + HHMM(0, 1) < session.current_time:
                    session.cancel_order(session.buy_orders[0].order_id)
    
    def on_update(self, session, updated_order):
        if updated_order.status == OrderStatus.Filled:
            if updated_order.order_side == OrderSide.Buy:
                self.has_position = True
            else:
                self.has_position = False

        print(updated_order)
    
    def on_account_update(self, session, account):
        print("account update: ", session.current_time, account)

    
market = BinanceMarket(BinanceConfig.TEST_BTCUSDT)

print(BinanceConfig.TEST_BTCUSDT)


agent = MyAgent()
runner = Runner()


init_debug_log()

def run():
    runner.real_run(market, agent, 60)


run()




    