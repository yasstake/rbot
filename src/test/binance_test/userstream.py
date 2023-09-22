from rbot import BinanceMarket, BinanceConfig

config = BinanceConfig.TESTSPOT("BTCUSDT")

binance = BinanceMarket(config)

binance.start_user_stream()
