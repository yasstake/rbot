

from rbot import Bybit, BybitConfig
from rbot import Runner
from rbot import init_debug_log
from handler import register


import uvicorn


if __name__ == "__main__":
    bybit = Bybit(False)
    market = bybit.open_market(BybitConfig.BTCUSDT)

    market.start_market_stream()
    
       
    register(market)
    
    print("START")
    
    uvicorn.run("handler:app", port=5000, log_level="info")
    