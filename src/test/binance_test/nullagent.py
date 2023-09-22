
from rbot import Session
from rbot import Runner
from rbot import BinanceConfig
from rbot import BinanceMarket
from rbot import init_debug_log
from rbot import init_log
from rbot import OrderSide

class MyAgent:
    def __init__(self):
        pass
    
    def on_clock(self):
        pass
    
    def on_tick(self, session, side, price, size):
        print("tick: ", session.current_time, side, price, size)
        
        market.new_limit_order(OrderSide.Sell, price + 100, 0.001)        
        pass
    
    def on_update(self, session, updated_order):
        print("buy order", session.buy_orders)
        print("sell order", session.sell_orders)
        print("account", session.account)
        print("update: ", session.current_time, updated_order)
        pass
    
init_log()
    
market = BinanceMarket(BinanceConfig.TEST_BTCUSDT)

print(BinanceConfig.TEST_BTCUSDT)

market.start_market_stream()
market.start_user_stream()
    
agent = MyAgent()
runner = Runner()
    
runner.run(market,agent)

    