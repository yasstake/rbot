


/* 
{'symbol': 'BTC/USDT', 'bids': [[53855.86, 1.105135], [53854.25, 0.009216], [53854.0, 0.0743], [53853.68, 0.772127], [53853.67, 0.355], [53853.66, 0.1], [53852.63, 0.009216], [53852.61, 0.355], [53852.0, 0.0743], [53851.57, 0.0002], [53851.0, 0.0002], [53850.51, 0.003], [53850.0, 0.075442], [53849.66, 0.011284], [53849.06, 0.018574], [53848.76, 0.529327], [53848.75, 0.029508], [53848.69, 0.001979], [53848.67, 0.011284], [53848.61, 0.243366], [53848.57, 0.009283], [53848.38, 7.2e-05], [53848.29, 0.792325], [53848.28, 0.364283], [53848.24, 0.014855], [53848.22, 0.037132], [53848.11, 0.037133], [53848.0, 0.0743], [53847.74, 0.037132], [53847.66, 0.036865], [53847.27, 0.011284], [53846.92, 0.051379], [53846.7, 0.0022], [53846.54, 0.033179], [53846.22, 0.02], [53846.18, 0.0265], [53845.65, 0.0002], [53844.51, 0.039499], [53844.43, 0.005978], [53844.06, 0.018569], [53844.0, 0.0743], [53843.92, 1.615594], [53843.91, 0.742801], [53843.7, 0.138735], [53843.0, 7.2e-05], [53842.3, 0.772132], [53842.29, 0.092688], [53841.99, 0.0002], [53841.82, 0.34], [53841.54, 0.000557]], 'asks': [[53855.87, 0.484711], [53856.0, 0.0743], [53856.88, 0.03949], [53857.48, 0.009264], [53857.62, 0.00239], [53857.95, 0.025854], [53857.99, 0.331598], [53858.0, 0.0743], [53858.41, 0.018575], [53858.43, 0.002634], [53858.62, 0.051379], [53859.0, 0.014855], [53859.1, 0.009216], [53859.26, 0.018574], [53859.36, 0.018569], [53859.37, 0.165915], [53859.39, 0.004636], [53859.4, 0.003212], [53859.55, 0.243366], [53860.0, 0.0743], [53860.22, 0.018575], [53860.62, 0.00258], [53861.03, 6.8e-05], [53861.1, 7.2e-05], [53861.25, 0.772129], [53861.26, 0.355], [53862.0, 0.0743], [53862.02, 0.005173], [53862.04, 0.005065], [53862.47, 0.003754], [53862.48, 0.02], [53863.24, 0.161608], [53863.25, 0.033289], [53863.63, 0.018575], [53864.0, 0.0743], [53864.52, 0.025785], [53864.74, 0.004372], [53865.23, 0.092688], [53865.24, 0.7808], [53865.87, 0.000365], [53866.0, 0.0743], [53866.27, 0.389414], [53866.49, 7.2e-05], [53866.7, 0.00258], [53866.97, 0.019064], [53866.98, 0.018528], [53868.0, 0.0743], [53868.34, 0.029867], [53868.81, 0.036945], [53869.47, 0.674543]], 'timestamp': 1725683542315, 'datetime': '2024-09-07T04:32:22.315Z', 'nonce': None}
*/

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::common::{string_to_decimal, BoardTransfer, LogStatus, OrderSide, Trade};

#[derive(Debug, Deserialize)]
pub struct CcxtOrderBook {
    symbol: String,
    bids: Vec<(f64, f64)>, // (価格, 数量)
    asks: Vec<(f64, f64)>, // (価格, 数量)
    timestamp: i64,
    datetime: String,
    nonce: Option<i64>, // nonceはオプション
}


impl Into<BoardTransfer> for CcxtOrderBook {
    fn into(self) -> BoardTransfer {
        todo!()
    }
}



/*
[
    {
        'info':          { ... },                  // the original decoded JSON as is
        'id':           '12345-67890:09876/54321', // string trade id
        'timestamp':     1502962946216,            // Unix timestamp in milliseconds
        'datetime':     '2017-08-17 12:42:48.000', // ISO8601 datetime with milliseconds
        'symbol':       'ETH/BTC',                 // symbol
        'order':        '12345-67890:09876/54321', // string order id or undefined/None/null
        'type':         'limit',                   // order type, 'market', 'limit' or undefined/None/null
        'side':         'buy',                     // direction of the trade, 'buy' or 'sell'
        'takerOrMaker': 'taker',                   // string, 'taker' or 'maker'
        'price':         0.06917684,               // float price in quote currency
        'amount':        1.5,                      // amount of base currency
        'cost':          0.10376526,               // total cost, `price * amount`,
        'fee':           {                         // if provided by exchange or calculated by ccxt
            'cost':  0.0015,                       // float
            'currency': 'ETH',                     // usually base currency for buys, quote currency for sells
            'rate': 0.002,                         // the fee rate (if available)
        },
        'fees': [                                  // an array of fees if paid in multiple currencies
            {                                      // if provided by exchange or calculated by ccxt
                'cost':  0.0015,                   // float
                'currency': 'ETH',                 // usually base currency for buys, quote currency for sells
                'rate': 0.002,                     // the fee rate (if available)
            },
        ]
    },
    ...
]

*/


#[derive(Serialize, Deserialize, Debug)]
pub struct CcxtTrade {
    info: Value,                // 'info':          { ... },                  // the original decoded JSON as is
    id: String,                                 //'id':           '12345-67890:09876/54321', // string trade id
    timestamp: i64, //'timestamp':     1502962946216,            // Unix timestamp in milliseconds
    datetime: String, //'datetime':     '2017-08-17 12:42:48.000', // ISO8601 datetime with milliseconds
    symbol: String, //'symbol':       'ETH/BTC',                 // symbol
    #[serde[rename="order"]]
    order: String, //'order':        '12345-67890:09876/54321', // string order id or undefined/None/null
    #[serde(rename="type")]
    order_type: String, //'type':         'limit',                   // order type, 'market', 'limit' or undefined/None/null
    side: String,   //'side':         'buy',                     // direction of the trade, 'buy' or 'sell'
    takerOrMaker: String, //'takerOrMaker': 'taker',                   // string, 'taker' or 'maker'
    #[serde(deserialize_with="string_to_decimal")]
    price: Decimal,     //'price':         0.06917684,               // float price in quote currency
    #[serde(deserialize_with="string_to_decimal")]
    amount: Decimal,    //'amount':        1.5,                      // amount of base currency
    #[serde(deserialize_with="string_to_decimal")]
    cost:   Decimal,    //'cost':          0.10376526,               // total cost, `price * amount`,
    fee: Value,//'fee':           {                         // if provided by exchange or calculated by ccxt
    //'cost':  0.0015,                       // float
    //'currency': 'ETH',                     // usually base currency for buys, quote currency for sells
    //'rate': 0.002,                         // the fee rate (if available)
//},
    fees: Value//'fees': [                                  // an array of fees if paid in multiple currencies
    //{                                      // if provided by exchange or calculated by ccxt
      //  'cost':  0.0015,                   // float
//        'currency': 'ETH',                 // usually base currency for buys, quote currency for sells
  //      'rate': 0.002,                     // the fee rate (if available)
//    },
//]
}



impl Into<Trade> for CcxtTrade {
    fn into(self) -> Trade {
        Trade {
            time: self.timestamp * 1_000,
            order_side: OrderSide::from(&self.side),
            price: self.price,
            size: self.amount,
            status: LogStatus::UnFix,
            id: self.id,
        }
    }
}


// https://github.com/ccxt/ccxt/wiki/Manual#ohlcv-structure
/*


[
    [
        1504541580000, // UTC timestamp in milliseconds, integer
        4235.4,        // (O)pen price, float
        4240.6,        // (H)ighest price, float
        4230.0,        // (L)owest price, float
        4230.7,        // (C)losing price, float
        37.72941911    // (V)olume float (usually in terms of the base currency, the exchanges docstring may list whether quote or base units are used)
    ],
    ...
]
*/






// https://github.com/ccxt/ccxt/wiki/Manual#order-structure
/*
{
'id':                '12345-67890:09876/54321', // string
'clientOrderId':     'abcdef-ghijklmnop-qrstuvwxyz', // a user-defined clientOrderId, if any
'datetime':          '2017-08-17 12:42:48.000', // ISO8601 datetime of 'timestamp' with milliseconds
'timestamp':          1502962946216, // order placing/opening Unix timestamp in milliseconds
'lastTradeTimestamp': 1502962956216, // Unix timestamp of the most recent trade on this order
'status':      'open',        // 'open', 'closed', 'canceled', 'expired', 'rejected'
'symbol':      'ETH/BTC',     // symbol
'type':        'limit',       // 'market', 'limit'
'timeInForce': 'GTC',         // 'GTC', 'IOC', 'FOK', 'PO'
'side':        'buy',         // 'buy', 'sell'
'price':        0.06917684,   // float price in quote currency (may be empty for market orders)
'average':      0.06917684,   // float average filling price
'amount':       1.5,          // ordered amount of base currency
'filled':       1.1,          // filled amount of base currency
'remaining':    0.4,          // remaining amount to fill
'cost':         0.076094524,  // 'filled' * 'price' (filling price used where available)
'trades':     [ ... ],        // a list of order trades/executions
'fee': {                      // fee info, if available
    'currency': 'BTC',        // which currency the fee is (usually quote)
    'cost': 0.0009,           // the fee amount in that currency
    'rate': 0.002,            // the fee rate (if available)
},
'info': { ... },              // the original unparsed order structure as is
}
*/


// https://github.com/ccxt/ccxt/wiki/Manual#accounts-structure

/*
[
    {
        id: "s32kj302lasli3930",
        type: "main",
        name: "main",
        code: "USDT",
        info: { ... }
    },
    {
        id: "20f0sdlri34lf90",
        name: "customAccount",
        type: "margin",
        code: "USDT",
        info: { ... }
    },
    {
        id: "4oidfk40dadeg4328",
        type: "spot",
        name: "spotAccount32",
        code: "BTC",
        info: { ... }
    },
    ...
]
*/
