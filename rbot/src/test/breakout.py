#
#import pandas as pd
import polars as pl

import rbot
from rbot import BaseAgent
from rbot import BinanceMarket
from rbot import BackRunner
from rbot import Market
from rbot import time_string
from rbot import _BinanceMarket



class BreakOutAgent(BaseAgent):
    """
        Agentのクラス名は任意。
        BaseAgentを継承し、clock_interval, on_clockを実装する。
    """

    def __init__(self, param_K=1.6):
            """ super().__init()__ で上位クラスの初期化を必ずすること　"""
            super().__init__()
            self.param_K = param_K  # パラメターKを設定する。

    def clock_interval(self):
        """ on_clockが呼び出される間隔を秒で返す。今回は10分毎(600秒)にする"""
        return 60 * 10
    
    def on_tick(self, time_us, session, side, price, size):
        pass
    
    def on_clock(self, time_us, session):
        """ Botのメインロジック。on_clockで設定した秒数毎に呼ばれる """
        # 前処理/ 前回のon_clock中でのオーダーが処理中の場合はなにもしない（リターン）        
        if session.short_order_size or session.long_order_size:
            return 
        
        ############   メインロジック  ###################### 
        ohlcv_df = session.ohlcv(60*60*2, 6)  # 2時間足(60*60*2sec)を６本取得。 最新は６番目。ただし未確定足
        if len(ohlcv_df) < 6:           # データが過去６本分そろっていない場合はなにもせずリターン
            return 

        ohlcv5 = ohlcv_df[:-2]       # 過去５本足（確定）
        range_width = (ohlcv5['high'] - ohlcv5['low']).mean() * self.param_K  # 価格変動レンジの平均を計算 * K

        # Long/Short判定
        ohlcv_latest = ohlcv_df[-2:-1]     # 最新足１本(未確定)        
        diff_low   =   (ohlcv_latest['open'][0] - ohlcv_latest['low'][0])
        detect_short  = range_width < diff_low

        diff_high  = - (ohlcv_latest['open'][0] - ohlcv_latest['high'][0])  
        detect_long = range_width  < diff_high
        
        ##########  メインロジック中に利用したindicatorのロギング（あとでグラフ化するため保存）    ##############
        self.log_indicator('diff_low', time_us, diff_low)
        self.log_indicator('diff_high', time_us, diff_high)
        self.log_indicator('range_width', time_us, range_width)

        ##########　執行戦略（順方向のポジションがあったら保留。逆方向のポジションがのこっていたらドテン）#########
        ORDER_SIZE = 0.01     # 標準オーダーサイズ(ドテンの場合はx2)
        ORDER_LIFE = 60*10    # オーダーの有効期間（60x10秒=10分)
        
        if detect_long and (not session.long_position_size): 
            if session.short_position_size: # Shortポジションがあった場合はドテン
                session.place_order( #オーダーを発行する
                            'Buy',                      # 'Buy', 'Sell'を選択
                            session.best_buy_price,     # 最後にbuyがtakeされた価格。sell側にはbest_sell_priceを提供。
                                                        # 任意の価格が設定できるがtakeになる価格でもmakeの処理・手数料で処理している。
                            ORDER_SIZE * 2,             # オーダーサイズ BTCBUSDの場合BTC建で指定。ドテンなので倍サイズでオーダー
                            ORDER_LIFE,                 # オーダーの有効期限（秒）。この秒数をこえるとExpireする。
                            'doten Long'                # あとでログで識別できるように任意の文字列が設定できる。
                )
            else:
                session.place_order('Buy', session.best_buy_price, ORDER_SIZE, ORDER_LIFE, 'Open Long')    

        if detect_short and (not session.short_position_size): # short判定のとき
            if session.long_position_size:  # Longポジションがあった場合はドテン
                session.place_order('Sell', session.best_sell_price, ORDER_SIZE * 2, ORDER_LIFE, 'Doten Short')
            else:
                session.place_order('Sell', session.best_sell_price, ORDER_SIZE, ORDER_LIFE, 'Open Short') 


binance = Market.open('BN', 'BTCBUSD')  # binance marketはあとで利用するので保存しておく
Market.download(50, False)       # 再ダウンロード


back_runner = BackRunner(
        'BN',           # Binance は BNと省略します。
        'BTCBUSD',      # 通貨ペアーを選択します。
        False           # 注文時に指定するサイズが通貨ペアーの右側通貨の場合True。BinanceのBTCBUSDはBTCでサイズを指定するのでFalse
)

back_runner.maker_fee_rate = 0.1 * 0.01  # maker_feeを指定（0.1%）. takerは未実装。現在は相手板にぶつける注文をしてもmaker_feeが適用される。


agent = BreakOutAgent()         # Agentのインスタンスを作ります（あとで利用するので変数に保存しておきます）。

#rbot.init_debug_log()

# 10日前から最新までバックテストする例。手元の環境で１日あたり10秒＋かかります。
back_runner.run(
    agent,                      # backtest するagentインスタンスを指定します。
    rbot.DAYS_BEFORE(100),       # 開始時刻を指定します(us)。0だとDBにある最初のデータから処理。DAYS_BEFOREはN日まえのtimestampを返すユーティリティ関数です。
#    0,
    0                           # 終了時刻を指定します(us). 0だとDBにある最後のデータまで処理。
)     

print(back_runner.result)                     # back testの結果概要が表示されます

