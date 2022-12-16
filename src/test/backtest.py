
import numpy as np
import pandas
import pandas as pd

import rbot
from rbot import init_log;
from rbot import Market
from rbot import Session
from rbot import BaseAgent
from rbot import BackRunner
from rbot import NOW
from rbot import DAYS
from rbot import OrderSide

#rbot.init_log()
#rbot.init_debug_log()

class Agent(BaseAgent):   
    def clock_interval(self):
        return 60    # Sec
    
    #def on_tick(self, session, time, price, side, size):
    #    print(session.current_timestamp)
    #    session.make_order(0, OrderSide.Buy, session.current_timestamp, 10.0, 100, "")

    def on_tick(self, time, session, price, side, size):
        pass

    def on_clock(self, time, session):
        pass
        #print(time)
        #
        # print(session.current_timestamp)
        #ohlcv = session.ohlcv(60, 100)
        
        #if session.long_order_size == 0 and session.long_position_size == 0:
        #    print(ohlcv)
        #    session.place_order("BUY", session.best_buy_price, 0.01, 600, "MyOrder")


    def on_update(self, time, session, result):
        print(str(result))
        pass

    

back_runner = BackRunner("BN", "BTCBUSD", False)

r = back_runner.run(Agent())

pd.options.display.max_rows=30
print(r)


