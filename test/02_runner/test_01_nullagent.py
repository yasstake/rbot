
from rbot import Session
from rbot import Runner

from rbot import Binance
from rbot import BinanceConfig

from rbot import init_debug_log
from rbot import init_log
from rbot import OrderSide
from rbot import time_string
from rbot import NOW, DAYS

class MyAgent:
    def __init__(self):
        pass

    def on_init(self, session):
        session.clock_interval_sec = 60
        #print("init: ", session.timestamp)

        pass
    
    def on_clock(self, session, clock):
        bid_edge, ask_edge = session.last_price
        #print("clock: ", time_string(session.timestamp), time_string(clock))
        #session.limit_order('Buy', bid_edge, 0.001)
        session.market_order('Buy', 0.001)        
        #session.limit_order('Sell', ask_edge, 0.001)        
        pass

    def on_tick(self, session, side, price, size):

        #print("tick: ", time_string(session.current_timestamp), side, price, size)
        pass
    
    def on_update(self, session, updated_order):
        pass;
        #print("ORDER update: ", updated_order.__str__())
        
    def on_account_update(self, session, account):
        #print("ACCOUNT update: ", session.timestamp, account, session.psudo_account)
        pass

import pytest
from rbot import Bybit

@pytest.mark.parametrize(
    "exchange, market_name",
    [
            (Bybit(False), "BTC/USDT:USDT"),
            (Bybit(False), "BTC/USDT"),
            (Binance(False), "BTC/USDT")
    ]
)

def test_null_agent_real_run(exchange, market_name):
    init_log()

    market = exchange.open_market(market_name)
    agent = MyAgent()
    runner = Runner()

    exchange.enable_order_with_my_own_risk = True

    runner = Runner()
    session = runner.real_run(
        exchange=exchange, market=market, agent=agent, verbose=True, execute_time=60*3, log_memory=True)

    print(session.log)


@pytest.mark.parametrize(
    "exchange, market_name",
    [
            (Bybit(False), "BTC/USDT:USDT"),
            (Bybit(False), "BTC/USDT"),
            (Binance(False), "BTC/USDT")
    ]
)
def test_null_agent_backtest(exchange, market_name):
    market = exchange.open_market(market_name)
    agent = MyAgent()
    runner = Runner()

    exchange.enable_order_with_my_own_risk = True

    runner = Runner()
    session = runner.back_test(
        exchange=exchange, market=market, agent=agent, verbose=True)

    print(session.log)


@pytest.mark.parametrize(
    "exchange, market_name",
    [
            (Bybit(True), "BTC/USDT:USDT"),
            (Bybit(True), "BTC/USDT"),
            (Binance(True), "BTC/USDT")
    ]
)
def test_null_agent_dryrun(exchange, market_name):
    init_log()

    market = exchange.open_market(BinanceConfig.BTCUSDT)
    agent = MyAgent()
    runner = Runner()

    exchange.enable_order_with_my_own_risk = True

    runner = Runner()
    session = runner.dry_run(
        exchange=exchange, market=market, agent=agent, verbose=True, execute_time=60*3, log_memory=True)

    print(session.log)



    