

from rbot import Bybit, BybitConfig
from rbot import Runner
from rbot import init_debug_log

bybit = Bybit(True)
market = bybit.open_market(BybitConfig.BTCUSDT)

runner = Runner()

#runner.start_proxy(market, 12455)

print("UDP start")


class MarketProxy:
    def __init__(self):
        self.exchange = {}

    def register_market(self, market):
        exchange = market.exchange_name
        category = market.config.trade_category
        symbol = market.config.trade_symbol
        
        key = f"{exchange}_{category}_{symbol}"
        
        self.exchange[key] = market    
        
    def get_board(cls, exchange_name, category, symbol):
        key = f"{exchange_name}_{category}_{symbol}"        

        self.exchange[key].board


proxy = MarketProxy()
proxy.register_market(market)


from fastapi import FastAPI

app = FastAPI()


@app.get("/board/{exchange_name}")
async def board(exchange_name):
    return {"message": "Hello World---", "exchange_name": exchange_name}

import uvicorn

if __name__ == "__main__":
    uvicorn.run("bybit_main:app", port=8000)
    