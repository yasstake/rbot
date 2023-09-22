
from rbot import Session
from rbot import Runner
from rbot import BinanceConfig
from rbot import BinanceMarket
from rbot import init_debug_log

class MyAgent:
    def __init__(self):
        pass
    
    def on_clock(self):
        pass
    
    def on_tick(self, session, side, price, size):
        print("tick: ", session.current_time, session.bids, session.asks, side, price, size)
        pass
    
    def on_update(self, session, updated_order):
        pass
    
init_debug_log()
    
market = BinanceMarket(BinanceConfig.TESTSPOT("BTCUSDT"))
market.start_market_stream()
    
agent = MyAgent()
runner = Runner()
    
runner.run(market,agent)

    