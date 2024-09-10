import ccxt


class CCXTApi:
    def __init__(self, exchange, key=None, secret=None, production=True):
        method = getattr(ccxt, exchange)
        
        self.ccxt = method()
        self.ccxt.load_markets()
        
        if production:
            self.ccxt.set_sandbox_mode(False)
        else:
            self.ccxt.set_sandbox_mode(True)
        
        if key:
            self.ccxt.apiKey = key
        if secret:
            self.ccxt.secret = secret     
        
    
    def get_board_snapshot(self, market: str): # -> anyhow::Result<BoardTransfer>;
        return self.ccxt.fetchOrderBook(market)

    def get_recent_trades(self, market: str): #  -> anyhow::Result<Vec<Trade>>;
        return self.ccxt.fetchTrades(market)

    def get_trades(
        self,
        market: str,
        start_time = None,
        from_id = None,
        page_id = None,
        #page: &RestPage
        ): # -> anyhow::Result<(Vec<Trade>, RestPage)>;
        if start_time:  
            return self.ccxt.fetchTrades(market, since=start_time)
        
        if from_id:
            params = {
                'from_id': from_id
            }
            return self.ccxt.fetchTrades(market, params)

    def get_klines(
        self,
        market: str,
        start_time = None,
        from_id = None,
        page_id = None,
    ): #-> anyhow::Result<(Vec<Kline>, RestPage)>;
        if start_time:  
            return self.ccxt.fetchOHLCV(market, timeframe='1m', since=start_time)
        
        if from_id:
            params = {
                'from_id': from_id
            }
            return self.ccxt.fetchOHLCV(market, timeframe='1m', params=params)
            

    # fn klines_width(&self) -> i64;
    def new_order(
        self,
        market: str,
        order_type: str,
        side: str, 
        price: float,
        size: float,
        client_order_id: str = None,
        ): # -> anyhow::Result<Vec<Order>>;
        return self.ccxt.createOrder(market, order_type, side, size, price)

    def cancel_order(self, market: str, order_id: str): # -> anyhow::Result<Order>;
        return self.ccxt.cancelOrder(order_id, market)

    def open_orders(self, market: str): # -> anyhow::Result<Vec<Order>>;
        return self.ccxt.fetchOpenOrders(market)

    def get_account(self): # -> anyhow::Result<AccountCoins>;
        return self.ccxt.fetchBalance()

    # def history_web_url(&self, config: &MarketConfig, date: MicroSec) -> String;
    # fn logdf_to_archivedf(&self, df: &DataFrame) -> anyhow::Result<DataFrame>;

    #async fn has_web_archive(&self, config: &MarketConfig, date: MicroSec) -> anyhow::Result<bool>
