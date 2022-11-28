from .rbot import *
import pandas as pd


if hasattr(rbot, "__all__"):
    __all__ = rbot.__all__


def decode_order_side(bs):
    if bs == 0:
        return "Sell"
    elif bs == 1:
        return "Buy"
    else:
        return "ERROR"


def decode_liquid(liq):
    if liq == 0:
        return False
    else:
        return True


def trades_to_df(array):
    df = pd.DataFrame(
        array, columns=["timestamp", "price", "size", "side", "liquid"])
    df['timestamp'] = pd.to_datetime(
        (df["timestamp"]), utc=True, unit='us')
    df = df.set_index('timestamp')

    df['side'] = df['side'].map(decode_order_side)
    df['liquid'] = df['liquid'].map(decode_liquid)

    return df


def ohlcvv_to_df(array):
    df = pd.DataFrame(
        array, columns=["timestamp", "order_side", "open", "high", "low", "close", "vol", "count", "start_time", "end_time"])
    df['timestamp'] = pd.to_datetime(
        (df["timestamp"]), utc=True, unit='us')
    df = df.set_index('timestamp')

    df['start_time'] = pd.to_datetime(
        (df["start_time"]), utc=True, unit='us')

    df['end_time'] = pd.to_datetime(
        (df["end_time"]), utc=True, unit='us')

    df['order_side'] = df['order_side'].map(decode_order_side)

    return df



def result_to_df(result_list):
    update_time = []
    order_id = []
    order_sub_id = []
    order_type = []
    post_only = []
    create_time = []
    status = []
    open_price = []
    close_price = []
    price = []
    size = []
    volume = []
    profit = []
    fee = []
    total_profit = []
    position_change = []
    message = []

    for item in result_list:
        update_time.append(item.update_time)
        order_id.append(item.order_id)
        order_sub_id.append(item.order_sub_id)
        order_type.append(item.order_type)
        post_only.append(item.post_only)
        create_time.append(item.create_time)
        status.append(item.status)
        open_price.append(item.open_price)
        close_price.append(item.close_price)
        price.append(item.price)
        size.append(item.size)
        volume.append(item.volume)
        profit.append(item.profit)
        fee.append(item.fee)
        total_profit.append(item.total_profit)
        position_change.append(item.position_change)
        message.append(item.message)

    df = pd.DataFrame(
    data={"update_time": update_time, "order_id": order_id, "sub_id": order_sub_id,
          "order_type": order_type, "post_only": post_only, "create_time": create_time,
          "status":  status, "open_price": open_price, "close_price": close_price,
          "price": price, "size": size, "volume": volume, "profit": profit, "fee": fee,
          "total_profit": total_profit, "pos_change": position_change, "message": message},
    columns=["update_time", "order_id", "sub_id", "order_type", "post_only",
             "create_time", "status", "open_price", "close_price", "price", "size", "volume",
             "profit", "fee", "total_profit", "pos_change", "message"])
    df["update_time"] = pd.to_datetime((df["update_time"]), utc=True, unit="ms")
    df["create_time"] = pd.to_datetime((df["create_time"]), utc=True, unit="ms")
    df["sum_profit"] = df["total_profit"].cumsum()
    df["sum_pos"] = df["pos_change"].cumsum()
    df = df.set_index("create_time", drop=True)

    return df


class BaseAgent:
    def __init__(self):
        self.session = None
    
    def initialize(self, session):
        self.session = session
    
    def clock_interval(self):
        return 60

    def _on_tick(self, time, session, price, side, size):
        self.on_tick(time, Session(session), price, side, size)

    def _on_clock(self, time, session):
        self.on_clock(time, Session(session))

    def _on_update(self, time, session, result):
        self.on_update(time, Session(session), result)


class Session:
    def __init__(self, session):
        self.session = session

    def __getattr__(self, func):
        return getattr(self.session, func)
        
    def ohlcv(self, time_window, num_of_bars, exchange_name=None, market_name=None):
        if not exchange_name:
            exchange_name = self.session.exchange_name
            market_name = self.session.market_name
        
        market = Market.get(exchange_name, market_name)

        now = self.session.current_timestamp

        return market.ohlcvv(now - time_window * num_of_bars * 1_000_000, now, time_window)




class Market:
    MARKET = {}
    DUMMY_MODE = True

    @classmethod
    def dummy_mode(cls, dummy=True):
        cls.DUMMY_MODE = dummy

    @classmethod
    def open(cls, exchange: str, market):
        exchange = exchange.upper()
        if exchange == "FTX":
            m = FtxMarket(market, cls.DUMMY_MODE)
            key = Market.key(exchange, market)            
            cls.MARKET[key] = m
            return m
        elif exchange == "BN":
            m = BinanceMarket(market, cls.DUMMY_MODE)
            key = Market.key(exchange, market)
            cls.MARKET[key] = m
            return m

    @classmethod
    def download(cls, ndays):
        for m in cls.MARKET:
            cls.MARKET[m].download(ndays)
    
    @classmethod
    def get(cls, exchange, market):
        key = Market.key(exchange, market)
        
        if key in cls.MARKET:
            return cls.MARKET[key]
        else:
            return Market.open(exchange, market)

    @staticmethod
    def key(exchange, market):
        return exchange.upper() + "/" + market.upper()

'''
class FtxMarket:
    def __init__(self, name, dummy=True):
        self.dummy = dummy
        self.ftx = _FtxMarket(name, dummy)
        self.exchange_name = "FTX"
        self.market_name = name

    def select_trades(self, from_time, to_time):
        return trades_to_df(self.ftx.select_trades(from_time, to_time))

    def ohlcvv(self, from_time, to_time, window_sec):
        return ohlcvv_to_df(self.ftx.ohlcvv(from_time, to_time, window_sec))

    def download(self, ndays, force=False):
        return self.ftx.download(ndays, force)

    def __getattr__(self, func):
        return getattr(self.ftx, func)
'''


class BinanceMarket:
    def __init__(self, name, dummy=True):
        self.dummy = dummy
        self.market = _BinanceMarket(name, dummy)
        self.exchange_name = "BN"
        self.market_name = name

    def select_trades(self, from_time, to_time):
        return trades_to_df(self.market.select_trades(from_time, to_time))

    def ohlcvv(self, from_time, to_time, window_sec):
        return ohlcvv_to_df(self.market.ohlcvv(from_time, to_time, window_sec))

    def download(self, ndays, force=False):
        return self.market.download(ndays, force)

    def __getattr__(self, func):
        return getattr(self.ftx, func)

