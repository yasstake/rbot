
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
        session.clock_interval = 60
        print("init: ", session.timestamp)

        pass
    
    def on_clock(self, session, clock):
        bid_edge, ask_edge = session.last_price
        print("clock: ", time_string(session.timestamp), time_string(clock))
        #session.limit_order('Buy', bid_edge, 0.001)
        session.market_order('Buy', 0.001)        
        #session.limit_order('Sell', ask_edge, 0.001)        
        pass


    def on_tick(self, session, side, price, size):

        #print("tick: ", time_string(session.current_timestamp), side, price, size)
        pass
    
    def on_update(self, session, updated_order):
        print("ORDER update: ", updated_order.__str__())
        
    def on_account_update(self, session, account):
        print("ACCOUNT update: ", session.timestamp, account, session.psudo_account)
        pass

init_log()
   
market = BinanceMarket(BinanceConfig.TEST_BTCUSDT)
#market.download(1, force=False, verbose=True)

market.cancel_all_orders()
    
agent = MyAgent()
runner = Runner()

#init_debug_log()
session = runner.back_test(market, agent, start_time=NOW()-DAYS(1), end_time=0, verbose=True,execute_time=60*3)
#session = runner.dry_run(market, agent, verbose=True, execute_time=60)
#session = runner.real_run(market, agent, verbose=True, execute_time=60)


print(session)




    