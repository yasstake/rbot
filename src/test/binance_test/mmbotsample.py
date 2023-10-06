# https://note.com/magimagi1223/n/n5fba7501dcfd

import datetime
import time

import ccxt
bitflyer = ccxt.bitflyer({
'apiKey': 'APIKEYを入力',
'secret': 'SECRETKEYを入力',
})

# 取引する通貨、シンボルを設定
COIN = 'BTC'
PAIR = 'BTCJPY28SEP2018'

# ロット(単位はBTC)
LOT = 0.002

# 最小注文数(取引所の仕様に応じて設定)
AMOUNT_MIN = 0.001

# スプレッド閾値
SPREAD_ENTRY = 0.0005  # 実効スプレッド(100%=1,1%=0.01)がこの値を上回ったらエントリー
SPREAD_CANCEL = 0.0003 # 実効スプレッド(100%=1,1%=0.01)がこの値を下回ったら指値更新を停止

# 数量X(この数量よりも下に指値をおく)
AMOUNT_THRU = 0.01

# 実効Ask/BidからDELTA離れた位置に指値をおく
DELTA = 1

#------------------------------------------------------------------------------#
#log設定
import logging
logger = logging.getLogger('LoggingTest')
logger.setLevel(10)
fh = logging.FileHandler('log_mm_bf_' + datetime.datetime.now().strftime('%Y%m%d') + '_' + datetime.datetime.now().strftime('%H%M%S') + '.log')
logger.addHandler(fh)
sh = logging.StreamHandler()
logger.addHandler(sh)
formatter = logging.Formatter('%(asctime)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)
sh.setFormatter(formatter)

#------------------------------------------------------------------------------#

# JPY残高を参照する関数
def get_asset():

    while True:
        try:
            value = bitflyer.fetch_balance()
            break
        except Exception as e:
            logger.info(e)
            time.sleep(1)
    return value

# JPY証拠金を参照する関数
def get_colla():

    while True:
        try:
            value = bitflyer.privateGetGetcollateral()
            break
        except Exception as e:
            logger.info(e)
            time.sleep(1)
    return value

# 板情報から実効Ask/Bid(=指値を入れる基準値)を計算する関数
def get_effective_tick(size_thru, rate_ask, size_ask, rate_bid, size_bid):

    while True:
        try:
            value = bitflyer.fetchOrderBook(PAIR)
            break
        except Exception as e:
            logger.info(e)
            time.sleep(2)

    i = 0
    s = 0
    while s <= size_thru:
        if value['bids'][i][0] == rate_bid:
            s += value['bids'][i][1] - size_bid
        else:
            s += value['bids'][i][1]
        i += 1

    j = 0
    t = 0
    while t <= size_thru:
        if value['asks'][j][0] == rate_ask:
            t += value['asks'][j][1] - size_ask
        else:
            t += value['asks'][j][1]
        j += 1

    time.sleep(0.5)
    return {'bid': value['bids'][i-1][0], 'ask': value['asks'][j-1][0]}

# 成行注文する関数
def market(side, size):

    while True:
        try:
            value = bitflyer.create_order(PAIR, type = 'market', side = side, amount = size)
            break
        except Exception as e:
            logger.info(e)
            time.sleep(2)

    time.sleep(0.5)
    return value

# 指値注文する関数
def limit(side, size, price):

    while True:
        try:
            value = bitflyer.create_order(PAIR, type = 'limit', side = side, amount = size, price = price)
            break
        except Exception as e:
            logger.info(e)
            time.sleep(2)

    time.sleep(0.5)
    return value

# 注文をキャンセルする関数
def cancel(id):

    try:
        value = bitflyer.cancelOrder(symbol = PAIR, id = id)
    except Exception as e:
        logger.info(e)

        # 指値が約定していた(=キャンセルが通らなかった)場合、
        # 注文情報を更新(約定済み)して返す
        value = get_status(id)

    time.sleep(0.5)
    return value

# 指定した注文idのステータスを参照する関数
def get_status(id):

    if PAIR == 'BTC/JPY':
        PRODUCT = 'BTC_JPY'
    else:
        PRODUCT = PAIR

    while True:
        try:
            value = bitflyer.private_get_getchildorders(params = {'product_code': PRODUCT, 'child_order_acceptance_id': id})[0]
            break
        except Exception as e:
            logger.info(e)
            time.sleep(2)

    # APIで受け取った値を読み換える
    if value['child_order_state'] == 'ACTIVE':
        status = 'open'
    elif value['child_order_state'] == 'COMPLETED':
        status = 'closed'
    else:
        status = value['child_order_state']

    # 未約定量を計算する
    remaining = float(value['size']) - float(value['executed_size'])

    time.sleep(0.1)
    return {'id': value['child_order_acceptance_id'], 'status': status, 'filled': value['executed_size'], 'remaining': remaining, 'amount': value['size'], 'price': value['price']}

#------------------------------------------------------------------------------#

# 未約定量が存在することを示すフラグ
remaining_ask_flag = 0
remaining_bid_flag = 0

# 指値の有無を示す変数
pos = 'none'

#------------------------------------------------------------------------------#

logger.info('--------TradeStart--------')
logger.info('BOT TYPE      : MarketMaker @ bitFlyer')
logger.info('SYMBOL        : {0}'.format(PAIR))
logger.info('LOT           : {0} {1}'.format(LOT, COIN))
logger.info('SPREAD ENTRY  : {0} %'.format(SPREAD_ENTRY * 100))
logger.info('SPREAD CANCEL : {0} %'.format(SPREAD_CANCEL * 100))

# 残高取得
asset = float(get_asset()['info'][0]['amount'])
colla = float(get_colla()['collateral'])
logger.info('--------------------------')
logger.info('ASSET         : {0}'.format(int(asset)))
logger.info('COLLATERAL    : {0}'.format(int(colla)))
logger.info('TOTAL         : {0}'.format(int(asset + colla)))

# メインループ
while True:

    # 未約定量の繰越がなければリセット
    if remaining_ask_flag == 0:
        remaining_ask = 0
    if remaining_bid_flag == 0:
        remaining_bid = 0

    # フラグリセット
    remaining_ask_flag = 0
    remaining_bid_flag = 0

    # 自分の指値が存在しないとき実行する
    if pos == 'none':

        # 板情報を取得、実効ask/bid(指値を入れる基準値)を決定する
        tick = get_effective_tick(size_thru=AMOUNT_THRU, rate_ask=0, size_ask=0, rate_bid=0, size_bid=0)
        ask = float(tick['ask'])
        bid = float(tick['bid'])
        # 実効スプレッドを計算する
        spread = (ask - bid) / bid

        logger.info('--------------------------')
        logger.info('ask:{0}, bid:{1}, spread:{2}%'.format(int(ask * 100) / 100, int(bid * 100) / 100, int(spread * 10000) / 100))

        # 実効スプレッドが閾値を超えた場合に実行する
        if spread > SPREAD_ENTRY:

            # 前回のサイクルにて未約定量が存在すれば今回の注文数に加える
            amount_int_ask = LOT + remaining_bid
            amount_int_bid = LOT + remaining_ask

            # 実効Ask/Bidからdelta離れた位置に指値を入れる
            trade_ask = limit('sell', amount_int_ask, ask - DELTA)
            trade_bid = limit('buy', amount_int_bid, bid + DELTA)
            trade_ask['status'] = 'open'
            trade_bid['status'] = 'open'
            pos = 'entry'

            logger.info('--------------------------')
            logger.info('entry')

            time.sleep(5)

    # 自分の指値が存在するとき実行する
    if pos == 'entry':

        # 注文ステータス取得
        if trade_ask['status'] != 'closed':
            trade_ask = get_status(trade_ask['id'])
        if trade_bid['status'] != 'closed':
            trade_bid = get_status(trade_bid['id'])

        # 板情報を取得、実効Ask/Bid(指値を入れる基準値)を決定する
        tick = get_effective_tick(size_thru=AMOUNT_THRU, rate_ask=float(trade_ask['price']), size_ask=float(trade_ask['amount']), rate_bid=float(trade_bid['price']), size_bid=float(trade_bid['amount']))
        ask = float(tick['ask'])
        bid = float(tick['bid'])
        spread = (ask - bid) / bid

        logger.info('--------------------------')
        logger.info('ask:{0}, bid:{1}, spread:{2}%'.format(int(ask * 100) / 100, int(bid * 100) / 100, int(spread * 10000) / 100))
        logger.info('ask status:{0}, filled:{1}/{2}, price:{3}'.format(trade_ask['status'], trade_ask['filled'], trade_ask['amount'], trade_ask['price']))
        logger.info('bid status:{0}, filled:{1}/{2}, price:{3}'.format(trade_bid['status'], trade_bid['filled'], trade_bid['amount'], trade_bid['price']))

        # Ask未約定量が最小注文量を下回るとき実行
        if trade_ask['status'] == 'open' and trade_ask['remaining'] <= AMOUNT_MIN:

            # 注文をキャンセル
            cancel_ask = cancel(trade_ask['id'])

            # ステータスをCLOSEDに書き換える
            trade_ask['status'] = 'closed'

            # 未約定量を記録、次サイクルで未約定量を加えるフラグを立てる
            remaining_ask = float(trade_ask['remaining'])
            remaining_ask_flag = 1

            logger.info('--------------------------')
            logger.info('ask almost filled.')

        # Bid未約定量が最小注文量を下回るとき実行
        if trade_bid['status'] == 'open' and trade_bid['remaining'] <= AMOUNT_MIN:

            # 注文をキャンセル
            cancel_bid = cancel(trade_bid['id'])

            # ステータスをCLOSEDに書き換える
            trade_bid['status'] = 'closed'

            # 未約定量を記録、次サイクルで未約定量を加えるフラグを立てる
            remaining_bid = float(trade_bid['remaining'])
            remaining_bid_flag = 1

            logger.info('--------------------------')
            logger.info('bid almost filled.')

        #スプレッドが閾値以上のときに実行する
        if spread > SPREAD_CANCEL:

            # Ask指値が最良位置に存在しないとき、指値を更新する
            if trade_ask['status'] == 'open' and trade_ask['price'] != ask - DELTA:

                # 指値を一旦キャンセル
                cancel_ask = cancel(trade_ask['id'])

                # 注文数が最小注文数より大きいとき、指値を更新する
                if trade_ask['remaining'] >= AMOUNT_MIN:
                    trade_ask = limit('sell', trade_ask['remaining'], ask - DELTA)
                    trade_ask['status'] = 'open'
                # 注文数が最小注文数より小さく0でないとき、未約定量を記録してCLOSEDとする
                elif AMOUNT_MIN > trade_ask['remaining'] > 0:
                    trade_ask['status'] = 'closed'
                    remaining_ask = float(trade_ask['remaining'])
                    remaining_ask_flag = 1
                # 注文数が最小注文数より小さく0のとき、CLOSEDとする
                else:
                    trade_ask['status'] = 'closed'

            # Bid指値が最良位置に存在しないとき、指値を更新する
            if trade_bid['status'] == 'open' and trade_bid['price'] != bid + DELTA:

                # 指値を一旦キャンセル
                cancel_bid = cancel(trade_bid['id'])

                # 注文数が最小注文数より大きいとき、指値を更新する
                if trade_bid['remaining'] >= AMOUNT_MIN:
                    trade_bid = limit('buy', trade_bid['remaining'], bid + DELTA)
                    trade_bid['status'] = 'open'
                # 注文数が最小注文数より小さく0でないとき、未約定量を記録してCLOSEDとする
                elif AMOUNT_MIN > trade_bid['remaining'] > 0:
                    trade_bid['status'] = 'closed'
                    remaining_bid = float(trade_bid['remaining'])
                    remaining_bid_flag = 1
                # 注文数が最小注文数より小さく0のとき、CLOSEDとする
                else:
                    trade_bid['status'] = 'closed'

        # Ask/Bid両方の指値が約定したとき、1サイクル終了、最初の処理に戻る
        if trade_ask['status'] == 'closed' and trade_bid['status'] == 'closed':
            pos = 'none'

            logger.info('--------------------------')
            logger.info('completed.')

    time.sleep(5)
