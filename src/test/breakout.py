
import pandas as pd

import rbot
from rbot import BaseAgent
from rbot import BinanceMarket
from rbot import BackTester
from rbot import time_string

class Agent(BaseAgent):
    def __init__(self, param_K=1.6):
            self.K = param_K                           # パラメターKを設定する。

    def clock_interval(self):
        #//return 60*60*2
        print("interval = 60")
        return 60
    
    #def on_tick(self, time_us, session, price, side, size):
    #    print(time_us, price, side, size)

    def on_clock(self, time_us, session):
        #print(time_us, session)
        
        print(time_string(time_us), session.buy_board_edge_price, session.sell_board_edge_price)        

        ohlcv_df = session.ohlcv(60*60*2, 6)       # ６本の足を取得。 最新は６番目。
        print(ohlcv_df)

        return 

        if len(ohlcv_df.index) < 6:                 # データが過去６本分そろっていない場合はなにもせずリターン
            return 

        print(time_string(time_us), session.buy_edge_price, session.sell_edge_price)

        if session.sell_edge_price < session.buy_edge_price:
            print("Error?")

        ohlcv_df["range"] = ohlcv_df["high"] - ohlcv_df["low"]      # レンジを計算

        ohlcv_latest = ohlcv_df[-2:-1]     # 最新足１本
        ohlcv_last_5 = ohlcv_df[:-2]       # 過去５本足

        range_width = ohlcv_last_5["range"].mean()      #　過去５本足のレンジの平均値

        # Long/Short判定
        detect_short = range_width * self.K < ohlcv_latest["high"][0] - ohlcv_latest["open"][0]
        detect_long  = range_width * self.K < ohlcv_latest["open"][0] - ohlcv_latest["low"][0]

        #　執行方法（順報告のポジションがあったら保留。逆方向のポジションがのこっていたらドテン）
        if detect_long:
            #    print("position", session.long_pos_size, session.short_pos_size)            
            #        print("make long")
            if not session.long_pos_size:
            #    print("makeorder")                                    
                if not session.short_pos_size:

                    return session.make_order("Buy", session.buy_edge_price, 100000, 600, "Open Long")    
                else:

                    return session.make_order("Buy", session.buy_edge_price, 200000, 600, "doten Long")    
            else:
                pass

        if detect_short:
            print("make short")            
            if not session.short_pos_size:
                print("makeorder")                                                      
                if not session.long_pos_size:
                    return session.make_order("Sell", session.sell_edge_price, 100000, 600, "Open Short") 
                else:
                    print("position", session.long_pos_size, session.short_pos_size)                    
                    return session.make_order("Sell", session.sell_edge_price, 200000, 600, "Doten Short") 
            else:
                pass

    def on_update(self, time, session, result):
        print(result)



#bn = BinanceMarket("BTCBUSD")
#bn.download(10)


back_tester = BackTester("BN", "BTCBUSD")

print("start")
r = back_tester.run(Agent())
print("end")

print(r)

