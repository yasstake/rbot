
from rbot import Session
from rbot import Runner
from rbot import BinanceConfig
from rbot import BinanceMarket
from rbot import init_debug_log
from rbot import init_log
from rbot import OrderSide
from rbot import time_string
from rbot import NOW, DAYS

class MyAgent:
    def __init__(self):
        pass

    def on_init(self, session):
        session.clock_interval = 60 * 60 
        print("init: ", session.timestamp)

        pass
    
    def on_clock(self, session, clock):
        bid_edge, ask_edge = session.last_price
        print("clock: ", time_string(session.timestamp), time_string(clock))
        session.limit_order('Buy', bid_edge, 0.001)
        session.limit_order('Sell', ask_edge, 0.001)        
        pass


    def on_tick(self, session, side, price, size):

        #print("tick: ", time_string(session.current_timestamp), side, price, size)
        pass
    
    def on_update(self, session, updated_order):
        print("update: ", updated_order.__str__())
        print("account", session.account)
        print("-------------------")
    
    def on_account_update(self, session, account):
        print("account update: ", session.timestamp, account)
        pass


   
market = BinanceMarket(BinanceConfig.BTCUSDT)
#market.start_market_stream()

#init_debug_log()

#market.analyze_db()

market.download(1, force=False, verbose=True)

#market.analyze_db()
#market.start_market_stream()

from time import sleep

    
agent = MyAgent()
runner = Runner()

session = runner.back_test(market, agent, start_time=NOW()-DAYS(1), end_time=0, verbose=True)

print(session)
#runner.dry_run(market, agent, interval_sec=60, verbose=True)


    