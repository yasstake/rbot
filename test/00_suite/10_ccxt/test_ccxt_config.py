import pytest

import json

import ccxt

from decimal import Decimal

def calculate_precision(value):
    # Decimalで数値を表現
    decimal_value = Decimal(str(value))
    # 有効桁数に基づいて精度を計算
    precision = Decimal('1e-{0}'.format(-decimal_value.as_tuple().exponent))
    
    return precision

class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'

class CCXTExchange:
    def __init__(self, exchange_name, production=True):
        self.exchange_name = exchange_name
        self.exchange = getattr(ccxt, self.exchange_name)()
        self.exchange.load_markets()
        self.production = production
        
        if not production:
            try:
                self.exchange.set_sandbox_mode(True)
            except Exception as e:
                return None

    def list_markets(self):
        market = []
        for m in self.exchange.markets:
            if (self.exchange.markets[m]['type'] != 'option') and (self.exchange.markets[m]['type'] != 'future'):
                market.append(m)
            
        return market
        
    def get_info(self):
        exchange_name = self.exchange.id
        host_name = self.exchange.hostname
        
        urls = self.exchange.urls 

        api = urls['api']
        
        host_map = SafeDict({"hostname": host_name})
        
        if 'public' in api:
            public_url = api['public'].format_map(host_map)
        elif 'rest' in api:
            public_url = api['rest'].format_map(host_map)
        elif 'common' in api:
            public_url = api['common'].format_map(host_map)
        else:
            raise('unknown public api')

        if 'private' in api:
            private_url = api['private'].format_map(host_map)
        elif 'rest' in api:
            private_url = api['rest'].format_map(host_map)
        elif 'common' in api:
            private_url = api['common'].format_map(host_map)
        else:
            raise('unknown private api')


        historical_web_base = '';
        public_ws_server = '';
        private_ws_server = '';

        if self.exchange_name == 'binance':
            historical_web_base = "https://data.binance.vision"
            if self.production:
                public_ws_server = "wss://stream.binance.com:9443/ws"            
                private_ws_server = "wss://stream.binance.com:9443"            
            else:
                public_ws_server = "wss://stream.binance.vision:9443/ws"
                private_ws_server = "wss://stream.binance.vision:9443"            

        elif self.exchange_name == 'bybit':
            historical_web_base = "https://public.bybit.com"
            if self.production:
                public_ws_server = "wss://stream.bybit.com/v5/public"
                private_ws_server = "wss://stream.bybit.com/v5/private"
            else:
                public_ws_server = "wss://stream-testnet.bybit.com/v5/public"
                private_ws_server = "wss://stream-testnet.bybit.com/v5/private"


        server_info = {
            'exchange_name': self.exchange_name,
            'production': self.production,
            'public_api': public_url,
            'private_api': private_url,
            'historical_web_base': historical_web_base,
            'public_ws_server': public_ws_server,
            'private_ws_server': private_ws_server
        }
        
        return server_info

    def home_currency(self, market):
        if 'quote' in market:
            return market['quote']
        
        print('no [quote] in market')
        
        if 'info' in market:
            info = market['info']
            if 'quoteCoin' in info:
                return info['quoteCoin']
            elif "quoteAsset" in info:
                return info['quoteAsset']
            elif "quote_asset" in info:
                return info['quote_asset']
            

        raise("unknown home currency")

    def foreign_currency(self, market):
        if 'base' in market:
            return market['base']

        print("no [base] in market")
        
        if 'info' in market:
            info = market['info']
            if 'baseCoin' in info:
                return info['baseCoin']
            elif 'baseAsset' in info:
                return info['baseAsset']
            elif 'base_asset' in info:
                return info['base_asset']
        

        raise("unknown foreign currency")


    def min_order_size(self, market):
        if 'info' in market:
            info = market['info']
            if 'lotSizeFilter' in info:
                lot_size_filter = info['lotSizeFilter']
            
                if 'minOrderQty' in lot_size_filter:
                    min_size = lot_size_filter['minOrderQty']
                    return float(min_size)
            elif 'filters' in info:
                filter = [f for f in info['filters'] if f['filterType'] == "LOT_SIZE"]
                if 'minQty'  in filter[0]:
                    min_size = filter[0]['minQty']
                    return float(min_size)

        return 0        

    def size_unit(self, market):
        if 'precision' in market:
            precision = market['precision']
            if 'amount' in precision:
                return precision['amount']

        if 'info' in market:
            info = market['info']
            if 'lotSizeFilter' in info:
                lot_size_filter = info['lotSizeFilter']
                if 'basePrecision' in lot_size_filter:
                    size_unit = lot_size_filter['basePrecision']
                else:
                    size_unit = calculate_precision(self.min_order_size(info))
                return size_unit                

            elif 'filters' in info:
                filter = [f for f in info['filters'] if f['filterType'] == "LOT_SIZE"]
                print("filter", filter)
                if 'stepSize'  in filter[0]:
                    size_unit = filter[0]['stepSize']
                
                return size_unit
        
            elif 'unit_amount' in info:
                return info['unit_amount']
        
        raise("unknown size unit")        

    def price_unit(self, market):
        if 'precision' in market:
            precision = market['precision']
            if 'price' in precision:
                return precision['price']


        if 'info' in market:
            info = market['info']

            if 'priceFilter' in info:
                return info['priceFilter']['tickSize']
            elif 'filters' in info:
                filter = [f for f in info['filters'] if f['filterType'] == "PRICE_FILTER"]
                if 'tickSize'  in filter[0]:
                    return filter[0]['tickSize']
            elif 'price_digits' in info:
                return 10 ** int(info['price_digits'])

        raise('unknown price unit')

    def get_fee_side(self):
        market = self.market
        if 'feeSide' in market:
            if market['feeSide'] == 'get':
                return 'home'
            else:
                print(market['feeSide'])
                return "foreign"
    
        if 'spot' in market:
            if market['spot']:
                return "home"
        
        if 'linear' in market:
            if market['linear']:
                return 'home'
        if 'inverse' in market:
            if market['inverse']:
                return 'foreign'
        
        return "UNKNOWN"

    def get_quote(self):
        market = self.market
        
        if 'quote' in market:
            return market['quote']
        
        raise("unknown quote")
    
    def get_settle(self):
        market = self.market
        
        if 'settle' in market:
            return market['settle']

        raise('unknown settle')
    
    def get_base(self):
        market = self.market
        if 'base' in market:
            return market['base']
        
        raise('unknown base')
        
    def open_market(self, unified_symbol):
        market = self.exchange.market(unified_symbol)
        self.market = market
        
        # --------- exchange_name -------------
        exchange_name = self.exchange.id
        
        # --------- market type --------------
        trade_category = market['type']
        if 'subType' in market:
            if market['subType']:
                trade_category = market['subType']
        
        
        info = market['info']
        
        # -------- trade_symbol ----------------        
        if 'symbol' in info:
            trade_symbol = info['symbol']
        elif 'name' in info:
            trade_symbol = info['name']
        elif 'product_code' in info:
            trade_symbol = info['product_code']
        else:
            raise('unknown trade symbol')
        
        # -------  asset -----------------------
        home_currency = self.home_currency(market)    
        foreign_currency = self.foreign_currency(market)    
        
        settle_currency = self.get_settle()
        quote_currency = self.get_quote()
        
        # ------ min_size -----------------------        
        min_size = self.min_order_size(market)

        # ------- size_unit -------------------
        size_unit = self.size_unit(market)

        if min_size == 0:
            min_size = size_unit

        # ------ price_unit ------------------
        price_unit = self.price_unit(market)

        #------- fee ------------------------        
        maker_fee = market['maker']
        taker_fee = market['taker']

        market_info = {
            'symbol': unified_symbol,
            'exchange_name': exchange_name,
            'trade_category': trade_category,
            'trade_symbol': trade_symbol,
            'home_currency': home_currency,
            'foreign_currency': foreign_currency,
            'quote_currency': quote_currency,
            'size_unit': size_unit,
            'min_size': min_size,
            'price_unit': price_unit,
            'maker_fee': maker_fee,
            'taker_fee': taker_fee,
        }

        if settle_currency != None:        
            market_info['settle_currency'] = settle_currency
        else:
            market_info['settle_currency'] = ""
        
        return market_info

@pytest.mark.parametrize(
    'exchange, production',
    [
            ('bitbank', True),
            ('bitflyer',True),
            ('binance', True),
            ('bitget', True),
            ('bybit', True),
    ]
)
def test_list_markets(exchange, production):
    exchange = CCXTExchange(exchange, production)        
    print(exchange.get_info())
    
    market = exchange.list_markets()
    print(market)

    for m in market:
        print(exchange.open_market(m))



def make_exchange_info(exchange_name, has_testnet):
    
    if has_testnet:
        exchange = CCXTExchange(exchange_name, False)        
        test_net = exchange.get_info()
    else:
        test_net = None

    exchange = CCXTExchange(exchange_name, True)
    production = exchange.get_info()

    market = exchange.list_markets()

    market_info = []
    for m in market:
        market_info.append(exchange.open_market(m))

    if has_testnet:
        exchange_info = {
            'exchange': exchange_name,
            'production': production,
            'testnet': test_net,
            'markets': market_info,
        }
    else:
        exchange_info = {
            'exchange': exchange_name,
            'production': production,
            'markets': market_info,
        }
    
    return exchange_info



"""
    {"bitbank":{
        'production': {},
        'testnet': {}
        'market': [{
            id="BTC/USDT:USDT",
        }]
    }
"""        
@pytest.mark.parametrize(
    'exchange_name, symbol',
    [
            ('bitbank', "BTC/JPY"),
            ('bitflyer',"BTC/JPY"),
            ('binance', "BTC/USDT"),
            ('bitget', "BTC/USDT"),
            ('bybit', "BTC/USDT"),
            ('binance', "BTC/USDT:USDT"),
            ('bitget', "BTC/USDT:USDT"),
            ('bybit', "BTC/USDT:USDT"),
            ('bitmex', "BTC/USD"),
    ]
)
def test_print_ccxt_market(exchange_name, symbol):
    exchange = CCXTExchange(exchange_name, True)        

    print(exchange.list_markets()) 
    
    exchange.open_market(symbol)
    print(exchange.market)   
    
    print(exchange.get_fee_side())


@pytest.mark.parametrize(
    'exchange_name, symbol',
    [
            ('bitflyer',"BTC/JPY"),
    ]
)
def test_print_ccxt_market2(exchange_name, symbol):
    exchange = CCXTExchange(exchange_name, True)        

    print(exchange.list_markets()) 
    
    exchange.open_market(symbol)
    print(exchange.market)   
    
    print(exchange.get_fee_side())
    print(exchange.get_settle())

    info = make_exchange_info(exchange_name, True)
    
    print(json.dumps(info, indent=4))
    
def main():
    info_list = []
    
    exchanges = [
            ('bybit', True),
            ('binance', True), 
            # ('bitflyer', False),
            ('bitbank', False),
            ('hyperliquid', True),            
            ]
    
    for ex, production in exchanges:
        info_list.append(make_exchange_info(ex, production))
    
    # print(json.dumps(info_list, indent=4))
    
    with open('/tmp/exchange.json', 'w') as file:
        json.dump(info_list, file, indent=4)
    

if __name__ == '__main__':
    main()
    
