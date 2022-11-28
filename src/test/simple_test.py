import numpy as np
import pandas
import pandas as pd

import rbot
from rbot import init_log;
from rbot import Market
from rbot import Session
from rbot import BaseAgent
from rbot import NOW
from rbot import DAYS
from rbot import _DummySession


init_log()
Market.dummy_mode()
Market.open("FTX", "BTC-PERP")
Market.download(1)


class Agent(BaseAgent):   
    def clock_interval(self):
        return 60    # Sec
    
    def on_tick(self, time, price, side, size):
        print("tick", time, price, side, size)
        print(session.current_timestamp)
        


session = _DummySession()
session.run(Agent(), 0, 0)


