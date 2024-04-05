# rbot (Rusty bot framwrok for crypto trading)

`rbot` is a python crypt trading bot framework written in Rust.

The feature is;
* Store historical transaction data(tick) in sqlite database.
* Calculate OHLCV data with any window sizefrom historical tick data
* Provide bot framework that enables `backtest`, `dry_run` and `production`.

### the execution sample

In the backtest mode, you can run with Jupyter notebook. So, you can make such analysis below. This example is provided in the github, which can be executed on the Google colab!! Please try.

[sample1: basil bot](https://github.com/yasstake/rusty-bot/blob/main/tutorial/basilbot/basilbot_backtest.ipynb)
[sample2: breakout bot](https://github.com/yasstake/rusty-bot/blob/main/tutorial/channel_breakout/breakout_agent.ipynb)

![backtest sample](https://github.com/yasstake/rbot/blob/87fff10f16b364816f4c6281a07e0bc7d338e486/img/backtest_sample.png?raw=true)




## Architecture

The only you have to make is `Agent`. The other class will support you Agent will work.

* `Session` will provide abstruction layer to the Market. Agent only interact with `Session` for ordering, fetching market information and current order status.
* `Market` is the API and DB wrapper for each trading pair in the exchange. At this version `Bybit` and `Biannce` exchange are supportd.
* `Runner` makes session to run `back_test`, `dry_run` or `real_run`.
* `Logger` stores the result of execution. That provide the result in polars `DataFrame`.

![architecture](https://github.com/yasstake/rbot/blob/87fff10f16b364816f4c6281a07e0bc7d338e486/img/rbot_outline.png?raw=true)

## install
```shell
$ pip install rbot
```

## download historical data


### create market object
```python
from rbot import Bybit, BybitConfig
exchange = Bybit(production=True)

config = BybitConfig.BTCUSDT          # use BTC/USDT pair
market = exchange.open_market(config) 
```

you can find the locatin of data base.
```python
market.file_name
```

In case you want to alter the location of db, you can specify the path by environment variable `RBOT_DB_ROOT`.

### enable order

Ordering is disabled by default. You can enable it by setting `enable_order_with_my_own_risk` to `True`.
```python
exchange.enable_order_with_my_own_risk = True
```


### download archive data

```python
market.download_archive(
    ndays=1,        # specify from past days
    force=False,    # if false, the cache data will be used.
    verbose=True    # verbose to print download progress.
)
```

### get OHLCV data

```python
ohlcv = market.ohlcv(
    start_time=0,           # start time in unix timestamp(microseconds)
    end_time=0,             # end time in unix timestamp(microseconds)
    window_sec=60           # ohlc bar window size in seconds
)
```
The OHLCV data is a polars data frames with the following columns:
* timestamp (in asc order)
* open
* high
* low
* close
* volume (sum of volume in order size)
* count (number of ticks)

if you want to use it as pandas dataframe, you can convert it by the following code.
```python
pandas_df = ohlcv.to_pandas(use_pyarrow_extension_array=True)
```


## Creating skelton bot 

You can make Bot class with any kind of names. Only rules is you must implement `on_init`, `on_tick`, `on_clock`, and `on_update`, if you want to recieve such event(It is not necessary to implement all of them).

Each method receives `session` object. You can use it to get market information, place order, cancel order, etc.

##### Example:
```python
class SkeltonAgent:      # you can use any names for trading bot agent / クラス名は任意です
    def on_init(self, session):
        """
        Bot initialization process. Bot initialization time. Called once at the start of the bot.
        It is best place to seting up session.clock_interval_sec which is interval of on_clock call.
        Args:
            session: Session class 
        """
        session.clock_interval_sec = 60 * 60 * 1        # 1時間ごとにon_clockを呼び出す

    def on_tick(self, session, side, price, size):
        """
        If you implement this method, you can receive all tick data from exchange.
        Args:
            session: Session object (that can be used to order and get market information)
            side: "Sell" or "Order"
            price: executed price of tick
            size: executed size of the tick
        """
        pass

    def on_clock(self, session, clock):
        """
        If you implement this method and seting up session.clock_interval_sec, 
        you can receive clock event in specified interval.
        Args:
            session: Session object(that can be used to order or get market information) 
            clock: Unix time stamp in micro seconds.
        """
        pass

    def on_update(self, session, updated_order):
        """
        If your order's status is changed, this method is called.
        Args:
            session: Session object
            updated_order: Updated order
        """
        pass
```
----


### Session API
In your `on_init`, `on_tick`, `on_clock`, and `on_update`, you can use `session` object to get market information, place order, cancel order, etc.

#### OHLCV data
```python
ohlcv = session.ohlcv(
    interval=60,        # ohlc bar window size in seconds
    count=10,           # number of bars to generate
)
```

The OHLCV data is a polars data frames with the following columns:
* timestamp (in asc order)
* open
* high
* low
* close
* volume (sum of volume in order size)
* count (number of ticks)


### order book
```python
bid, ask = session.board
```
`bid`, `ask` are polars dataframes with the following columns:
* price: price of the order
* size: size of the order
* sum: cumulative size of the order


### place order
#### market order
```python
size = 0.001
market_order = session.market_order("BUY", size)
```

### limit order

```python
price = 50000.0
size = 0.001
sell_limit_order = session.limit_order("SELL", price, size)
```

### cancel order
```python
cancelled_order =session.cancel_order(config, id_to_cancel)
```


### order queue status
You can get the order queue status(Limit orders that is not fullfied)
```python
buy_orders = session.buy_orders
sell_orders = session.sell_orders
```

### expire order
Expire the order in the order queue.
```python
expire_time = 60 * 60   # expire older than 1H
session.expire_order(expire_time)
```

### position
calculate psudo-position from the session starts.
```python
position = session.position
```

---

## Running bot

You can run Agent without modification in three modes; `backtest`, `dry_run`, and `production`.


### backtest

```python
from rbot import Runner

agent = SkeltonAgent()
runner = Runner()

session = runner.back_test(
                exchange=exchange,  # exchange object
                market=market,    # market object
                agent=agent,      # agent object
                start_time=0,    # start time in unix timestamp(microseconds)` 0 means from the beginig of DB
                end_time=0,      # end time in unix timestamp(microseconds) 0 means to the end of the DB
                verbose=True     # verbose to print progress.
            )
```

### dry run
```python
from rbot import Runner

agent = SkeltonAgent()
runner = Runner()

session = runner.dry_run(
                exchange=exchange,  # exchange object
                market=market,    # market object
                agent=agent,      # agent object
                execute_time = 60,  # Time(Seconds) to execute
                verbose=True     # verbose to print progress.
            )
```

### real run(production)

```python
from rbot import Runner

agent = SkeltonAgent()
runner = Runner()

session = runner.real_run(
                exchange=exchange,  # exchange object
                market=market,    # market object
                agent=agent,      # agent object
                #execute_time = 60,  # execute time, if not set, it runs forever
                verbose=True,
                log_file="skelton_bot.log"
            )
```

---
## analize bot performance

you can get orders dataframe list by `session.orders`.

* log_id	
* symbol	
* update_time	
* create_time	
* status	
* order_id	
* client_order_id	
* order_side	
* order_type	
* order_price	
* order_size	
* remain_size	
* transaction_id	
* execute_price	
* execute_size	
* quote_vol	
* commission	
* commission_asset	
* is_maker	
* message	
* commission_home	
* commission_foreign	
* home_change	
* foreign_change	
* free_home_change	
* free_foreign_change	
* lock_home_change	
* lock_foreign_change	
* open_position	
* close_position	
* position	
* profit	
* fee	
* total_profit	
* sum_profit

## note

for real run mode, you can retreive log as below
```python
from rbot import Logger

log = Logger()
log.restore("skelton_bot.log")
```

---
# Changes from release-0.2
* Exchange and market is separated. So you must make Exchange(such as `Bybit`) first, then `open_market` to build market object.
* `Market#download` is separated into three `Market#download_archive`, `Market#download_latest` and `Market#donwload_gap`
* `Runner#real_run` and `Runner#dry_run` now have `no_download` optional flag.
* Support `Bybit` exchange.


---

## links

[rbot github](https://github.com/yasstake/rbot)

[Example github](https://github.com/yasstake/rusty-bot)
[API manuals](https://github.com/yasstake/rusty-bot/blob/main/manual/manual.ipynb)

---
copyright(c) 2024 yasstake. 
All rights reserved.
Distributed under LGPL license.
NOTE: For some echange, it may have a such kind of an affliate link.
