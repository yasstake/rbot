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
from rbot import NOW
from rbot import DAYS
from rbot import HHMM
from rbot import time_string

class MyAgent:
    def __init__(self, market):
        self.market = market
        self.last_update = 0
        self.order_size = 0.001
        self.has_position = False        
    
    def on_tick(self, session, side, price, size):
        vap = self.market.vap(session.current_time, 
                              session.current_time+ HHMM(0, 10), 5)
        print("on_tick", time_string(session.current_time), side, price, size, vap)
        
market = BinanceMarket(BinanceConfig.BTCUSDT)        

agent = MyAgent(market)
runner = Runner()

runner.back_test(market, agent, NOW()-DAYS(3), 0, 0)

