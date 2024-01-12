
from fastapi import FastAPI, Depends

from typing import Optional


class Market:
    instance = None

    def __new__(cls):
        if cls.instance is None:
            cls.instance = super().__new__(cls)
            cls.instance.MARKET = {}
        return cls.instance

    def register(self, market):
        key = self.key(market.exchange_name, market.config.trade_category, market.config.trade_symbol)
        self.MARKET[key] = market

    def key(self, exchange_name, category, symbol):
        return f"{exchange_name}_{category}_{symbol}"

    def get_board(self, exchange_name, category, symbol, limit: int = 100):
        key = self.key(exchange_name, category, symbol)
        
        if key in self.MARKET.keys():
            return self.MARKET[key].get_board_json(limit)
        else:
            return None


def get_market() -> Optional[Market]:
    return Market()

app = FastAPI()


@app.get("/")
def read_root():
    keys = Market().MARKET.keys()
    print(keys)
    return {"message"}


@app.get("/board/{exchange_name}/{category}/{symbol}")
async def board(exchange_name, category, symbol, limit: int = 100):
    market = Market()    
    key = f"{exchange_name}_{category}_{symbol}"
    
    return market.get_board(exchange_name, category, symbol)


def register(exchange):
    market = Market()
    market.register(exchange)
