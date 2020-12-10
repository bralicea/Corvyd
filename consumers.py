import faust
from datetime import datetime
import zlib
import time
import json
import base64
import instruments

# Dictionary meant to normalize data type for the 'direction' field in database
normalizeDirectionField = {False: 'buy', '1': 'buy', 'buy': 'buy', 'b': 'buy', 'bid': 'buy', 1: 'buy', 'false': 'buy',
                           True: 'sell', '2': 'sell', 'sell': 'sell', 's': 'sell', 'ask': 'sell', 2: 'sell', 0: 'sell', 'true': 'sell'}

# Start Faust
app = faust.App('hello-app', broker='localhost:9092')

Trades = app.topic('Trades')
OrderBooks = app.topic('OrderBooks')

biboxTrades = app.topic('biboxTrades')
biboxOrderBooks = app.topic('biboxOrderBooks')

binanceTrades = app.topic('binanceTrades')
binanceOrderBooks = app.topic('binanceOrderBooks')

binanceUsTrades = app.topic('binanceUsTrades')
binanceUsOrderBooks = app.topic('binanceUsOrderBooks')

bikiTrades = app.topic('bikiTrades', value_serializer='raw')
bikiOrderBooks = app.topic('bikiOrderBooks', value_serializer='raw')

bitfinexTrades = app.topic('bitfinexTrades')
bitfinexOrderBooks = app.topic('bitfinexOrderBooks')

bitflyerTrades = app.topic('bitflyerTrades')
bitflyerOrderBooks = app.topic('bitflyerOrderBooks')

bitforexTrades = app.topic('bitforexTrades')
bitforexOrderBooks = app.topic('bitforexOrderBooks')

bitmexTrades = app.topic('bitmexTrades')
bitmexOrderBooks = app.topic('bitmexOrderBooks')

bitstampTrades = app.topic('bitstampTrades')
bitstampOrderBooks = app.topic('bitstampOrderBooks')

bittrexTrades = app.topic('bittrexTrades', value_serializer='raw')
bittrexOrderBooks = app.topic('bittrexOrderBooks', value_serializer='raw')

coinbaseTrades = app.topic('coinbaseTrades')
coinbaseOrderBooks = app.topic('coinbaseOrderBooks')

coinexTrades = app.topic('coinexTrades')
coinexOrderBooks = app.topic('coinexOrderBooks')

deribitTrades = app.topic('deribitTrades')
deribitOrderBooks = app.topic('deribitOrderBooks')

gateTrades = app.topic('gateTrades')
gateOrderBooks = app.topic('gateOrderBooks')

geminiTrades = app.topic('geminiTrades')
geminiOrderBooks = app.topic('geminiOrderBooks')

hitbtcTrades = app.topic('hitbtcTrades')
hitbtcOrderBooks = app.topic('hitbtcOrderBooks')

huobiTrades = app.topic('huobiTrades', value_serializer='raw')
huobiOrderBooks = app.topic('huobiOrderBooks', value_serializer='raw')

krakenTrades = app.topic('krakenTrades')
krakenOrderBooks = app.topic('krakenOrderBooks')

kucoinTrades = app.topic('kucoinTrades')
okexTrades = app.topic('okexTrades', value_serializer='raw')
phemexTrades = app.topic('phemexTrades')
poloniexTrades = app.topic('poloniexTrades')
zbTrades = app.topic('zbTrades')

# Returns order book bids and asks lists
def getLists(bids, asks, priceFormat, amountFormat):
    bidsList = []
    asksList = []

    # Get first 20 entries if it exists, or full list otherwise
    for cnt, bid in enumerate(bids):
        bidsList.append({'price': float(bid[priceFormat]), 'amount': float(bid[amountFormat])})
        if cnt == 19:
            break

    for cnt, ask in enumerate(asks):
        asksList.append({'price': float(ask[priceFormat]), 'amount': float(ask[amountFormat])})
        if cnt == 19:
            break

    return bidsList, asksList

# Ingest trade data from Druid
@app.agent(Trades)
async def trades(dataList):
    async for data in dataList:
        print(data)

# Ingest order book data from Druid
@app.agent(OrderBooks)
async def orderbooks(dataList):
    async for data in dataList:
        print(data)

@app.agent(biboxTrades)
async def biboxtrades(msgs):
    async for msg in msgs:
        if isinstance(msg, list):
            # decompress, decode, then jsonify msg
            msg = json.loads((zlib.decompress(base64.b64decode(msg[0]['data']), zlib.MAX_WBITS | 32)).decode('utf-8'))
            for submsg in msg:
                exchange = 'bibox'
                pair = submsg['pair'].replace('_', '').lower()
                amount = float(submsg['amount'])
                price = float(submsg['price'])
                direction = normalizeDirectionField[submsg['side']]
                ts = int(submsg['time'])

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

@app.agent(biboxOrderBooks)
async def biboxorderbooks(msgs):
    async for msg in msgs:
        try:
            # decompress, decode, then jsonify msg
            msg = json.loads((zlib.decompress(base64.b64decode(msg[0]['data']), zlib.MAX_WBITS | 32)).decode('utf-8'))
            exchange = 'bibox'
            pair = msg['pair'].replace('_', '').lower()
            ts = int(msg['update_time'])
            bids, asks = getLists(msg['bids'], msg['asks'], 'price', 'volume')

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "time": ts,
                "bids": bids,
                "asks": asks
            }

            await orderbooks.send(value=json_body)

        except:
            pass

@app.agent(binanceTrades)
async def binancetrades(msgs):
    async for msg in msgs:
        msg = msg['data']
        exchange = "binance"
        pair = msg['s'].lower()
        amount = float(msg['q'])
        price = float(msg['p'])
        if msg['m'] == True:
            direction = "sell"
        else:
            direction = "buy"
        ts = int(msg['T'])

        json_body = {
            "exchange": exchange,
            "pair": pair,
            "direction": direction,
            "time": ts,
            "amount": amount,
            "price": price
        }

        await trades.send(value=json_body)

@app.agent(binanceOrderBooks)
async def binanceorderbooks(msgs):
    async for msg in msgs:
        exchange = 'binance'
        pair = msg['stream'].split('@')[0]
        ts = time.time_ns() // 1000000
        bids, asks = getLists(msg['data']['bids'], msg['data']['asks'], 0, 1)

        json_body = {
            "exchange": exchange,
            "pair": pair,
            "time": ts,
            "bids": bids,
            "asks": asks
        }

        await orderbooks.send(value=json_body)

@app.agent(binanceUsTrades)
async def binanceustrades(msgs):
    async for msg in msgs:
        msg = msg['data']
        exchange = 'binanceus'
        pair = msg['s'].lower()
        amount = float(msg['q'])
        price = float(msg['p'])
        if msg['m'] == True:
            direction = "sell"
        else:
            direction = "buy"
        ts = msg['T']

        json_body = {
            "exchange": exchange,
            "pair": pair,
            "direction": direction,
            "time": ts,
            "amount": amount,
            "price": price
        }

        await trades.send(value=json_body)

@app.agent(binanceUsOrderBooks)
async def binanceusorderbooks(msgs):
    async for msg in msgs:
        exchange = 'binanceus'
        pair = msg['stream'].split('@')[0]
        ts = time.time_ns() // 1000000
        bids, asks = getLists(msg['data']['bids'], msg['data']['asks'], 0, 1)

        json_body = {
            "exchange": exchange,
            "pair": pair,
            "time": ts,
            "bids": bids,
            "asks": asks
        }

        await orderbooks.send(value=json_body)

@app.agent(bikiTrades)
async def bikitrades(msgs):
    async for msg in msgs:
        msg = json.loads(zlib.decompress(msg, zlib.MAX_WBITS | 32))
        if 'event_rep' in msg:
            exchange = 'biki'
            pair = msg['channel'].split('_')[1]
            amount = float(msg['tick']['data'][0]['vol'])
            price = float(msg['tick']['data'][0]['price'])
            direction = normalizeDirectionField[msg['tick']['data'][0]['side'].lower()]
            ts = int(msg['tick']['data'][0]['ts'])

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "direction": direction,
                "time": ts,
                "amount": amount,
                "price": price
            }

            await trades.send(value=json_body)

@app.agent(bikiOrderBooks)
async def bikiorderbooks(msgs):
    async for msg in msgs:
        msg = json.loads(zlib.decompress(msg, zlib.MAX_WBITS | 32))
        if 'event_rep' in msg:
            exchange = 'biki'
            pair = msg['channel'].split('_')[1]
            ts = int(msg['ts'])
            bids, asks = getLists(msg['tick']['buys'], msg['tick']['asks'], 0, 1)

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "time": ts,
                "bids": bids,
                "asks": asks
            }

            await orderbooks.send(value=json_body)

@app.agent(bitfinexTrades)
async def bitfinextrades(msgs):
    # Bitfinex generates new pair IDs every connection
    bitfinexId = {}
    async for msg in msgs:
        try:
            if 'event' in msg and msg['event'] == 'subscribed':
                # Read current IDs
                f = open("bitfinexId.json", "r+")
                fileDict = json.loads(f.read())
                f.close()

                # Open in write mode to clear log and add dictionary with new ID
                f = open("bitfinexId.json", "w")
                fileDict[msg['chanId']] = msg['symbol']
                f.write(json.dumps(fileDict))
                f.close()

                # Add id to dictionary
                bitfinexId[msg['chanId']] = msg['symbol']

            elif len(msg) == 3:
                exchange = 'bitfinex'
                # Try to read ID from dictionary. If it doesn't exist, read from file and then add to dictionary
                try:
                    pair = bitfinexId[msg[0]][1::].lower()
                except:
                    f = open("bitfinexId.json", "r+")
                    fileDict = json.loads(f.read())
                    pair = fileDict[str(msg[0])][1::].lower()
                    bitfinexId[msg[0]] = fileDict[str(msg[0])]
                    f.close()
                amount = float(abs(msg[2][2]))
                price = float(msg[2][3])
                if amount >= 0:
                    direction = "buy"
                else:
                    direction = "sell"
                ts = int(msg[2][1])

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

        except:
            pass

@app.agent(bitfinexOrderBooks)
async def bitfinexorderbooks(msgs):
    # Bitfinex generates new pair IDs every connection
    bitfinexOBId = {}
    OB = {}
    async for msg in msgs:
        try:
            if 'event' in msg and msg['event'] == 'subscribed':
                # Read current IDs
                f = open("bitfinexOBId.json", "r+")
                fileDict = json.loads(f.read())
                f.close()

                # Open in write mode to clear log and add dictionary with new ID
                f = open("bitfinexOBId.json", "w")
                fileDict[msg['chanId']] = msg['symbol']
                f.write(json.dumps(fileDict))
                f.close()

                # Add id to dictionary
                bitfinexOBId[msg['chanId']] = msg['symbol']
                OB[msg['symbol']] = {'exchange': 'bitfinex', 'pair': msg['symbol'][1::], 'time': time.time_ns() // 1000000, 'bids': [], 'asks': []}

            # Snapshots
            elif len(msg) == 2 and isinstance(msg[1][0], list):
                for data in msg[1]:
                    # Add to bids
                    if data[2] >= 0:
                        if data[1] >= 1:
                            price = float(data[0])
                            amount = float(data[2])
                            bid = {'price': price, 'amount': amount}
                            OB[bitfinexOBId[msg[0]]]['bids'].append(bid)
                            OB[bitfinexOBId[msg[0]]]['time'] = time.time_ns() // 1000000
                            break

                        elif data[1] == 0:
                            for count, bid in enumerate(OB[bitfinexOBId[msg[0]]]['bids']):
                                if bid['price'] == data[0]:
                                    del OB[bitfinexOBId[msg[0]]]['bids'][count]
                                    OB[bitfinexOBId[msg[0]]]['time'] = time.time_ns() // 1000000
                                    break

                    # Add to asks
                    else:
                        if data[1] == 1:
                            price = float(data[0])
                            amount = abs(float(data[2]))
                            ask = {'price': price, 'amount': amount}
                            OB[bitfinexOBId[msg[0]]]['asks'].append(ask)
                            break

                        elif data[1] == 0:
                            for count, ask in enumerate(OB[bitfinexOBId[msg[0]]]['asks']):
                                if ask['price'] == data[0]:
                                    del OB[bitfinexOBId[msg[0]]]['asks'][count]
                                    break

                await orderbooks.send(value=OB[bitfinexOBId[msg[0]]])

            # Updates
            elif len(msg) == 2 and isinstance(msg[1][0], float):
                # Add to bids
                if msg[1][2] >= 0:
                    if msg[1][1] >= 1:
                        price = float(msg[1][0])
                        amount = float(msg[1][2])
                        bid = {'price': price, 'amount': amount}
                        OB[bitfinexOBId[msg[0]]]['bids'].append(bid)
                        OB[bitfinexOBId[msg[0]]]['time'] = time.time_ns() // 1000000

                    elif msg[1][1] == 0:
                        for count, bid in enumerate(OB[bitfinexOBId[msg[0]]]['bids']):
                            if bid['price'] == msg[1][0]:
                                del OB[bitfinexOBId[msg[0]]]['bids'][count]
                                OB[bitfinexOBId[msg[0]]]['time'] = time.time_ns() // 1000000
                                break

                # Add to asks
                else:
                    if msg[1][1] == 1:
                        price = float(msg[1][0])
                        amount = abs(float(msg[1][2]))
                        ask = {'price': price, 'amount': amount}
                        OB[bitfinexOBId[msg[0]]]['asks'].append(ask)

                    elif msg[1][1] == 0:
                        for count, ask in enumerate(OB[bitfinexOBId[msg[0]]]['asks']):
                            if ask['price'] == msg[1][0]:
                                del OB[bitfinexOBId[msg[0]]]['asks'][count]
                                break

                await orderbooks.send(value=OB[bitfinexOBId[msg[0]]])

        except:
            pass

@app.agent(bitflyerTrades)
async def bitflyertrades(msgs):
    async for msg in msgs:
        for data in msg['params']['message']:
            exchange = 'bitflyer'
            pairFormat = msg['params']['channel'].split('_')
            pair = (pairFormat[2] + pairFormat[3]).lower()
            amount = data['size']
            price = data['price']
            direction = data['side'].lower()
            dt = datetime.strptime(data['exec_date'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds()) * 1000

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "direction": direction,
                "time": ts,
                "amount": amount,
                "price": price
            }

            await trades.send(value=json_body)

@app.agent(bitflyerOrderBooks)
async def bitflyerorderbooks(msgs):
    async for msg in msgs:
        exchange = 'bitflyer'
        pairFormat = msg['params']['channel'].split('_')
        pair = (pairFormat[3] + pairFormat[4]).lower()
        ts = time.time_ns() // 1000000
        bids, asks = getLists(msg['params']['message']["bids"], msg['params']['message']["asks"], 'price', 'size')

        json_body = {
            "exchange": 'bitflyer',
            "pair": pair,
            "time": ts,
            "bids": bids,
            "asks": asks
        }

        await orderbooks.send(value=json_body)

@app.agent(bitforexTrades)
async def bitforextrades(msgs):
    async for msg in msgs:
        try:
            exchange = 'bitforex'
            pairFormat = msg['param']['businessType'].split('-')
            pair = pairFormat[2] + pairFormat[1]
            amount = float(msg['data'][0]['amount'])
            price = float(msg['data'][0]['price'])
            direction = normalizeDirectionField[str(msg['data'][0]['direction'])]
            ts = int(msg['data'][0]['time'])

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "direction": direction,
                "time": ts,
                "amount": amount,
                "price": price
            }

            await trades.send(value=json_body)

        except:
            pass

@app.agent(bitforexOrderBooks)
async def bitforexorderbooks(msgs):
    async for msg in msgs:
        if 'bids' in msg['data'] and 'asks' in msg['data']:
            exchange = 'bitforex'
            pairFormat = msg['param']['businessType'].split('-')
            pair = pairFormat[2] + pairFormat[1]
            ts = time.time_ns() // 1000000
            bids, asks = getLists(msg['data']['bids'], msg['data']['asks'], 'price', 'amount')

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "time": ts,
                "bids": bids,
                "asks": asks
            }

            await orderbooks.send(value=json_body)

@app.agent(bitmexTrades)
async def bitmextrades(msgs):
    async for msg in msgs:
        if 'data' in msg:
            msg = msg['data'][0]
            exchange = 'bitmex'
            if 'XBT' in msg['symbol']:
                pair = msg['symbol'].replace('XBT', 'btc').lower()
            else:
                pair = msg['symbol']
            amount = float(msg['size'])
            price = float(msg['price'])
            direction = normalizeDirectionField[msg['side'].lower()]
            dt = datetime.strptime(msg['timestamp'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds()) * 1000

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "direction": direction,
                "time": ts,
                "amount": amount,
                "price": price
            }

            await trades.send(value=json_body)

@app.agent(bitmexOrderBooks)
async def bitmexorderbooks(msgs):
    async for msg in msgs:
        if 'table' in msg:
            exchange = 'bitmex'
            if 'XRP' in msg['data'][0]['symbol']:
                pair = msg['data'][0]['symbol'].replace('XRP', 'btc').lower()
            else:
                pair = msg['data'][0]['symbol'].lower()
            dt = datetime.strptime(msg['data'][0]['timestamp'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds()) * 1000
            bids, asks = getLists(msg['data'][0]['bids'], msg['data'][0]['asks'], 0, 1)

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "time": ts,
                "bids": bids,
                "asks": asks
            }

            await orderbooks.send(value=json_body)

@app.agent(bitstampTrades)
async def bitstamptrades(msgs):
    async for msg in msgs:
        if 'buy_order_id' in msg['data']:
            exchange = 'bitstamp'
            pair = msg['channel'].split('_')[2]
            amount = msg['data']['amount']
            price = msg['data']['price']
            if msg['data']['type'] == 0:
                direction = 'buy'
            else:
                direction = 'sell'
            ts = int(msg['data']['microtimestamp'])

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "direction": direction,
                "time": ts,
                "amount": amount,
                "price": price
            }

            await trades.send(value=json_body)

@app.agent(bitstampOrderBooks)
async def bitstamporderbooks(msgs):
    async for msg in msgs:
        if 'microtimestamp' in msg['data']:
            exchange = 'bitstamp'
            pair = msg['channel'].split('_')[2]
            ts = msg['data']['microtimestamp']
            bids, asks = getLists(msg['data']['bids'], msg['data']['asks'], 0, 1)

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "time": ts,
                "bids": bids,
                "asks": asks
            }

            await orderbooks.send(value=json_body)

@app.agent(bittrexTrades)
async def bittrextrades(msgs):
    async for msg in msgs:
        decompress_msg = zlib.decompress(base64.b64decode(msg, validate=True), -zlib.MAX_WBITS)
        msg = json.loads(decompress_msg.decode('utf-8'))
        if msg['f'] != []:
            for data in msg['f']:
                exchange = 'bittrex'
                pairFormat = msg['M'].split('-')
                pair = (pairFormat[1] + pairFormat[0]).lower()
                amount = data['Q']
                price = data['R']
                direction = data['OT'].lower()
                ts = data['T']

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

@app.agent(bittrexOrderBooks)
async def bittrexorderbooks(msgs):
    async for msg in msgs:
        try:
            msg = json.loads(msg.decode('utf-8'))

            exchange = 'bittrex'
            pairFormat = msg['M'].split('-')
            pair = (pairFormat[1] + pairFormat[0]).lower()
            ts = int((msg['timestamp'] * 1000) // 1)

            bids, asks = getLists(msg['Z'], msg['S'], 'R', 'Q')

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "time": ts,
                "bids": bids,
                "asks": asks
            }

            await orderbooks.send(value=json_body)

        except:
            pass

@app.agent(coinexTrades)
async def coinextrades(msgs):
    async for msg in msgs:
        if 'method' in msg:
            exchange = 'coinex'
            pair = msg['params'][0].lower()
            for ms in msg['params'][1]:
                amount = float(ms['amount'])
                price = float(ms['price'])
                direction = normalizeDirectionField[ms['type']]
                ts = int(ms['time'] * 1000 // 1)

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

@app.agent(coinexOrderBooks)
async def coinexorderbooks(msgs):
    ob = {}
    async for msg in msgs:
        try:
            if msg['params'][0] == True:
                ob[msg['params'][-1]] = {'bids': {}, 'asks': {}}
                for bid in msg['params'][1]['bids']:
                    ob[msg['params'][-1]]['bids'][float(bid[0])] = bid[1]
                for ask in msg['params'][1]['asks']:
                    ob[msg['params'][-1]]['asks'][float(ask[0])] = ask[1]

            else:
                if 'bids' in msg['params'][1]:
                    for bid in msg['params'][1]['bids']:
                        if float(bid[0]) in ob[msg['params'][-1]]['bids']:
                            if bid[1] == '0':
                                del ob[msg['params'][-1]]['bids'][float(bid[0])]

                            else:
                                ob[msg['params'][-1]]['bids'][float(bid[0])] = bid[1]

                        else:
                            ob[msg['params'][-1]]['bids'][float(bid[0])] = bid[1]

                if 'asks' in msg['params'][1]:
                    for ask in msg['params'][1]['asks']:
                        if float(ask[0]) in ob[msg['params'][-1]]['asks']:
                            if ask[1] == '0':
                                del ob[msg['params'][-1]]['asks'][float(ask[0])]

                            else:
                                ob[msg['params'][-1]]['asks'][float(ask[0])] = ask[1]

                        else:
                            ob[msg['params'][-1]]['asks'][float(ask[0])] = ask[1]

                bidsList = []
                asksList = []

                for bid in sorted(ob[msg['params'][-1]]['bids'])[::-1]:
                    bidsList.append({'price': bid, 'amount': float(ob[msg['params'][-1]]['bids'][bid])})

                for ask in sorted(ob[msg['params'][-1]]['asks']):
                    asksList.append({'price': ask, 'amount': float(ob[msg['params'][-1]]['asks'][ask])})

                exchange = 'coinex'
                pair = msg['params'][-1].lower()
                ts = msg['params'][1]['time']
                bids = bidsList
                asks = asksList

                json_body = {
                    'exchange': exchange,
                    'pair': pair,
                    'time': ts,
                    'bids': bids,
                    'asks': asks
                }

                await orderbooks.send(value=json_body)

        except:
            pass

@app.agent(coinbaseTrades)
async def coinbasetrades(msgs):
    async for msg in msgs:
        if msg['type'] == 'match':
            exchange = 'coinbase'
            pair = msg['product_id'].lower()
            amount = float(msg['size'])
            price = float(msg['price'])
            direction = msg['side']
            dt = datetime.strptime(msg['time'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = (int((dt - datetime.utcfromtimestamp(0)).total_seconds()) * 1000) + int(msg['time'].split('.')[1][0:3])

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "direction": direction,
                "time": ts,
                "amount": amount,
                "price": price
            }

            await trades.send(value=json_body)

@app.agent(coinbaseOrderBooks)
async def coinbaseorderbooks(msgs):
    ob = {}
    async for msg in msgs:
        try:
            if msg['type'] == 'snapshot':
                ob[msg['product_id']] = {'bids': {}, 'asks': {}}
                for bid in msg['bids'][0:20]:
                    ob[msg['product_id']]['bids'][float(bid[0])] = bid[1]
                for ask in msg['asks'][0:20]:
                    ob[msg['product_id']]['asks'][float(ask[0])] = ask[1]

            else:
                for change in msg['changes']:
                    if change[0] == 'buy':
                        if change[2] == '0.00000000':
                            if float(change[1]) in ob[msg['product_id']]['bids']:
                                del ob[msg['product_id']]['bids'][float(change[1])]

                        else:
                            ob[msg['product_id']]['bids'][float(change[1])] = change[2]

                    else:
                        if change[2] == '0.00000000':
                            if float(change[1]) in ob[msg['product_id']]['asks']:
                                del ob[msg['product_id']]['asks'][float(change[1])]

                        else:
                            ob[msg['product_id']]['asks'][float(change[1])] = change[2]

            bidsList = []
            asksList = []

            for cnt, bid in enumerate(sorted(ob[msg['product_id']]['bids'])[::-1]):
                bidsList.append({'price': bid, 'amount': float(ob[msg['product_id']]['bids'][bid])})
                if cnt == 19:
                    break

            for cnt, ask in enumerate(sorted(ob[msg['product_id']]['asks'])):
                asksList.append({'price': ask, 'amount': float(ob[msg['product_id']]['asks'][ask])})
                if cnt == 19:
                    break

            exchange = 'coinbase'
            pairFormat = msg['product_id'].split('-')
            pair = (pairFormat[0] + pairFormat[1]).lower()
            dt = datetime.strptime(msg['time'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = (int((dt - datetime.utcfromtimestamp(0)).total_seconds()) * 1000) + int(msg['time'].split('.')[1][0:3])
            bids = bidsList
            asks = asksList

            json_body = {
                'exchange': exchange,
                'pair': pair,
                'time': ts,
                'bids': bids,
                'asks': asks
            }

            await orderbooks.send(value=json_body)

        except Exception as e:
            print(e)

@app.agent(deribitTrades)
async def deribittrades(msgs):
    async for msg in msgs:
        try:
            for data in (msg['params']['data']):
                exchange = 'deribit'
                pair = data['instrument_name'].split('-')[0].lower() + 'usd'
                direction = data['direction']
                ts = data['timestamp']
                price = data['mark_price']
                amount = data['amount']

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

        except:
            pass

@app.agent(deribitOrderBooks)
async def deribitorderbooks(msgs):
    ob = {"ETH-PERPETUAL": {'bids': {}, 'asks': {}, 'time': 0}, "BTC-PERPETUAL": {'bids': {}, 'asks': {}, 'time': 0}}
    async for msg in msgs:
        try:
            if len(msg['params']['data']['bids']) > 0:
                for bid in msg['params']['data']['bids']:
                    if bid[0] == 'new' or bid[0] == 'change':
                        ob[msg['params']['data']['instrument_name']]['bids'][float(bid[1])] = bid[2]

                    elif bid[0] == 'delete':
                        del ob[msg['params']['data']['instrument_name']]['bids'][float(bid[1])]

            if len(msg['params']['data']['asks']) > 0:
                for ask in msg['params']['data']['asks']:
                    if ask[0] == 'new' or ask[0] == 'change':
                        ob[msg['params']['data']['instrument_name']]['asks'][float(ask[1])] = ask[2]

                    elif ask[0] == 'delete':
                        del ob[msg['params']['data']['instrument_name']]['asks'][float(ask[1])]

            bidsList = []
            asksList = []

            for cnt, bid in enumerate(sorted(ob[msg['params']['data']['instrument_name']]['bids'])[::-1]):
                bidsList.append({'price': bid, 'amount': float(ob[msg['params']['data']['instrument_name']]['bids'][bid])})
                if cnt == 19:
                    break

            for cnt, ask in enumerate(sorted(ob[msg['params']['data']['instrument_name']]['asks'])):
                asksList.append({'price': ask, 'amount': float(ob[msg['params']['data']['instrument_name']]['asks'][ask])})
                if cnt == 19:
                    break

            exchange = 'deribit'
            pair = msg['params']['data']['instrument_name'].split('-')[0].lower() + 'usd'
            ts = msg['params']['data']['timestamp']
            bids = bidsList
            asks = asksList

            json_body = {
                'exchange': exchange,
                'pair': pair,
                'time': ts,
                'bids': bids,
                'asks': asks
            }

            await orderbooks.send(value=json_body)

        except:
            pass

@app.agent(gateTrades)
async def gatetrades(msgs):
    async for msg in msgs:
        if 'method' in msg:
            exchange = 'gate'
            pairFormat = msg['params'][0].split('_')
            pair = (pairFormat[0] + pairFormat[1]).lower()
            for submsg in msg['params'][1]:
                amount = float(submsg['amount'])
                price = float(submsg['price'])
                direction = normalizeDirectionField[submsg['type']]
                ts = int(submsg['time'] // 1) * 1000

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

@app.agent(gateOrderBooks)
async def gateorderbooks(msgs):
    ob = {}
    async for msg in msgs:
        try:
            if msg['params'][0] == True:
                ob[msg['params'][-1]] = {'bids': {}, 'asks': {}}
                for bid in msg['params'][1]['bids']:
                    ob[msg['params'][-1]]['bids'][float(bid[0])] = bid[1]
                for ask in msg['params'][1]['asks']:
                    ob[msg['params'][-1]]['asks'][float(ask[0])] = ask[1]

            else:
                if 'bids' in msg['params'][1]:
                    for bid in msg['params'][1]['bids']:
                        if float(bid[0]) in ob[msg['params'][-1]]['bids']:
                            if bid[1] == '0':
                                del ob[msg['params'][-1]]['bids'][float(bid[0])]

                            else:
                                ob[msg['params'][-1]]['bids'][float(bid[0])] = bid[1]

                        else:
                            ob[msg['params'][-1]]['bids'][float(bid[0])] = bid[1]

                if 'asks' in msg['params'][1]:
                    for ask in msg['params'][1]['asks']:
                        if float(ask[0]) in ob[msg['params'][-1]]['asks']:
                            if ask[1] == '0':
                                del ob[msg['params'][-1]]['asks'][float(ask[0])]

                            else:
                                ob[msg['params'][-1]]['asks'][float(ask[0])] = ask[1]

                        else:
                            ob[msg['params'][-1]]['asks'][float(ask[0])] = ask[1]

                bidsList = []
                asksList = []

                for bid in sorted(ob[msg['params'][-1]]['bids'])[::-1]:
                    bidsList.append({'price': bid, 'amount': float(ob[msg['params'][-1]]['bids'][bid])})

                for ask in sorted(ob[msg['params'][-1]]['asks']):
                    asksList.append({'price': ask, 'amount': float(ob[msg['params'][-1]]['asks'][ask])})

                exchange = 'coinex'
                pairFormat = msg['params'][-1].split('_')
                pair = (pairFormat[0] + pairFormat[1]).lower()
                ts = time.time_ns() // 1000000
                bids = bidsList
                asks = asksList

                json_body = {
                    'exchange': exchange,
                    'pair': pair,
                    'time': ts,
                    'bids': bids,
                    'asks': asks
                }

                await orderbooks.send(value=json_body)

        except:
            pass

@app.agent(geminiTrades)
async def geminitrades(msgs):
    async for msg in msgs:
        try:
            exchange = 'gemini'
            pair = msg['symbol'].lower()
            amount = msg['quantity']
            price = msg['price']
            direction = msg['side']
            ts = msg['timestamp']

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "direction": direction,
                "time": ts,
                "amount": amount,
                "price": price
            }

            await trades.send(value=json_body)

        except:
            pass

@app.agent(geminiOrderBooks)
async def geminiorderbooks(msgs):
    ob = {}
    async for msg in msgs:
        if msg['symbol'] not in ob:
            ob[msg['symbol']] = {'bids': {}, 'asks': {}}

        for change in msg['changes']:
            if change[0] == 'buy':
                if change[2] != '0':
                    ob[msg['symbol']]['bids'][float(change[1])] = change[2]

                elif change[2] == '0' and float(change[1]) in ob[msg['symbol']]['bids']:
                    del ob[msg['symbol']]['bids'][float(change[1])]

            if change[0] == 'sell':
                if change[2] != '0':
                    ob[msg['symbol']]['asks'][float(change[1])] = change[2]

                elif change[2] == '0' and float(change[1]) in ob[msg['symbol']]['asks']:
                    del ob[msg['symbol']]['asks'][float(change[1])]

        bidsList = []
        asksList = []

        for cnt, bid in enumerate(sorted(ob[msg['symbol']]['bids'])[::-1]):
            bidsList.append({'price': bid, 'amount': float(ob[msg['symbol']]['bids'][bid])})
            if cnt == 19:
                break

        for cnt, ask in enumerate(sorted(ob[msg['symbol']]['asks'])):
            asksList.append({'price': ask, 'amount': float(ob[msg['symbol']]['asks'][ask])})
            if cnt == 19:
                break

        exchange = 'gemini'
        pair = msg['symbol'].lower()
        ts = time.time_ns() // 1000000
        bids = bidsList
        asks = asksList

        json_body = {
            'exchange': exchange,
            'pair': pair,
            'time': ts,
            'bids': bids,
            'asks': asks
        }

        await orderbooks.send(value=json_body)

@app.agent(hitbtcTrades)
async def hitbtctrades(msgs):
    async for msg in msgs:
        if 'params' in msg:
            for data in msg['params']['data']:
                exchange = 'hitbtc'
                pair = msg['params']['symbol'].lower()
                amount = float(data['quantity'])
                price = float(data['price'])
                direction = data['side']
                dt = datetime.strptime(data['timestamp'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
                ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds()) * 1000

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

@app.agent(hitbtcOrderBooks)
async def hitbtcorderbooks(msgs):
    ob = {}
    async for msg in msgs:
        try:
            if msg['method'] == 'snapshotOrderbook':
                ob[msg['params']['symbol']] = {'bids': {}, 'asks': {}}
                for bid in msg['params']['bid'][0:25]:
                    ob[msg['params']['symbol']]['bids'][float(bid['price'])] = bid['size']
                for ask in msg['params']['ask'][0:25]:
                    ob[msg['params']['symbol']]['asks'][float(ask['price'])] = ask['size']

            elif msg['method'] == 'updateOrderbook':
                for bid in msg['params']['bid']:
                    if float(bid['size']) == 0 and float(bid['price']) in ob[msg['params']['symbol']]['bids']:
                        del ob[msg['params']['symbol']]['bids'][float(bid['price'])]

                    elif float(bid['size']) > 0:
                        ob[msg['params']['symbol']]['bids'][float(bid['price'])] = bid['size']

                for ask in msg['params']['ask']:
                    if float(ask['size']) == 0 and float(ask['price']) in ob[msg['params']['symbol']]['asks']:
                        del ob[msg['params']['symbol']]['asks'][float(ask['price'])]

                    elif float(ask['size']) > 0:
                        ob[msg['params']['symbol']]['asks'][float(ask['price'])] = ask['size']

            bidsList = []
            asksList = []

            for cnt, bid in enumerate(sorted(ob[msg['params']['symbol']]['bids'])[::-1]):
                bidsList.append({'price': float(bid), 'amount': float(ob[msg['params']['symbol']]['bids'][bid])})
                if cnt == 19:
                    break

            for cnt, ask in enumerate(sorted(ob[msg['params']['symbol']]['asks'])):
                asksList.append({'price': float(ask), 'amount': float(ob[msg['params']['symbol']]['asks'][ask])})
                if cnt == 19:
                    break

            exchange = 'hitbtc'
            pair = msg['params']['symbol'].lower()
            dt = datetime.strptime(msg['params']['timestamp'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = (int((dt - datetime.utcfromtimestamp(0)).total_seconds()) * 1000) + int(msg['params']['timestamp'].split('.')[1][0:3])
            bids = bidsList
            asks = asksList

            json_body = {
                'exchange': exchange,
                'pair': pair,
                'time': ts,
                'bids': bids,
                'asks': asks
            }

            await orderbooks.send(value=json_body)

        except:
            pass

@app.agent(huobiTrades)
async def huobitrades(msgs):
    async for msg in msgs:
        msg = json.loads(zlib.decompress(msg, zlib.MAX_WBITS | 32))
        if 'ch' in msg:
            for data in msg['tick']['data']:
                exchange = 'huobi'
                pair = msg['ch'].split('.')[1]
                amount = data['amount']
                price = data['price']
                direction = data['direction']
                ts = data['ts']

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

@app.agent(huobiOrderBooks)
async def huobiorderbooks(msgs):
    async for msg in msgs:
        try:
            msg = json.loads(zlib.decompress(msg, zlib.MAX_WBITS | 32))

            exchange = 'huobi'
            pair = msg['ch'].split('.')[1]
            ts = msg['ts']
            bids, asks = getLists(msg['tick']['bids'][0:20], msg['tick']['asks'][0:20], 0, 1)

            json_body = {
                'exchange': exchange,
                'pair': pair,
                'time': ts,
                'bids': bids,
                'asks': asks
            }

            await orderbooks.send(value=json_body)

        except:
            pass

@app.agent(krakenTrades)
async def krakentrades(msgs):
    async for msg in msgs:
        if len(msg) == 4 and isinstance(msg, list):
            for data in msg[1]:
                exchange = 'kraken'
                if 'XBT' in msg[3]:
                    msg[3] = msg[3].replace('XBT', 'BTC')
                pair = msg[3].replace('/', '').lower()
                amount = float(data[1])
                price = float(data[0])
                direction = normalizeDirectionField[data[3]]
                ts = int(float(data[2])//1) * 1000

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

@app.agent(krakenOrderBooks)
async def krakenorderbooks(msgs):
    ob = {}
    async for msg in msgs:
        try:
            if 'as' in msg[1]:
                ob[msg[-1]] = {'bids': {}, 'asks': {}}
                for ask in msg[1]['as']:
                    ob[msg[-1]]['asks'][float(ask[0])] = ask[1]

            if 'bs' in msg[1]:
                for bid in msg[1]['as']:
                    ob[msg[-1]]['bids'][float(bid[0])] = bid[1]

            elif 'a' in msg[1]:
                for ask in msg[1]['a']:
                    if float(ask[1]) == 0:
                        del ob[msg[-1]]['asks'][float(ask[0])]

                    else:
                        ob[msg[-1]]['asks'][float(ask[0])] = ask[1]

            elif 'b' in msg[1]:
                ts = msg[1]['b'][0]
                for bid in msg[1]['b']:
                    if float(bid[1]) == 0:
                        del ob[msg[-1]]['bids'][float(bid[0])]

                    else:
                        ob[msg[-1]]['bids'][float(bid[0])] = bid[1]

            bidsList = []
            asksList = []

            for cnt, bid in enumerate(sorted(ob[msg[-1]]['bids'])[::-1]):
                bidsList.append({'price': float(bid), 'amount': float(ob[msg[-1]]['bids'][bid])})
                if cnt == 19:
                    break

            for cnt, ask in enumerate(sorted(ob[msg[-1]]['asks'])):
                asksList.append({'price': float(ask), 'amount': float(ob[msg[-1]]['asks'][ask])})
                if cnt == 19:
                    break

            exchange = 'kraken'
            pair = msg[-1].replace('/', '').lower()
            if 'xbt' in pair:
                pair = pair.replace('xbt', 'btc')
            bids = bidsList
            asks = asksList
            ts = time.time_ns() // 1000000

            json_body = {
                'exchange': exchange,
                'pair': pair,
                'time': ts,
                'bids': bids,
                'asks': asks
            }

            await orderbooks.send(value=json_body)

        except:
            pass

@app.agent(kucoinTrades)
async def kucointrades(msgs):
    async for msg in msgs:
        if 'data' in msg and 'sequence' in msg['data']:
            msg = msg['data']
            exchange = 'kucoin'
            pair = msg['symbol'].replace('-', '').lower()
            amount = float(msg['size'])
            price = float(msg['price'])
            direction = msg['side']
            ts = int(msg['time'])//1000000

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "direction": direction,
                "time": ts,
                "amount": amount,
                "price": price
            }

            await trades.send(value=json_body)

@app.agent(okexTrades)
async def okextrades(msgs):
    async for msg in msgs:
        msg = json.loads(zlib.decompress(msg, -zlib.MAX_WBITS | 32))
        if 'table' in msg:
            msg = msg['data'][0]
            exchange = 'okex'
            pairFormat = msg['instrument_id'].split('-')
            pair = (pairFormat[0] + pairFormat[1]).lower()
            amount = float(msg['size'])
            price = float(msg['price'])
            direction = normalizeDirectionField[msg['side']]
            dt = datetime.strptime(msg['timestamp'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds()) * 1000

            json_body = {
                "exchange": exchange,
                "pair": pair,
                "direction": direction,
                "time": ts,
                "amount": amount,
                "price": price
            }

            await trades.send(value=json_body)

# Phemex needs to fix their amount and price formats
@app.agent(phemexTrades)
async def phemextrades(msgs):
    async for msg in msgs:
        if 'sequence' in msg:
            for data in msg['trades']:
                exchange = 'phemex'
                pair = msg['symbol'][1::].lower()
                amount = data[3]
                price = data[2]
                direction = data[1].lower()
                ts = data[0] // 1000000

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

@app.agent(poloniexTrades)
async def poloniextrades(msgs):
    async for msg in msgs:
        if len(msg) >= 3:
            for submsg in msg[2]:
                if submsg[0] == 't':
                    exchange = 'poloniex'
                    pairFormat = (instruments.poloniexId[msg[0]]).split('_')
                    pair = (pairFormat[1] + pairFormat[0]).lower()
                    amount = float(submsg[4])
                    price = float(submsg[3])
                    direction = normalizeDirectionField[submsg[2]]
                    ts = submsg[5] * 1000

                    json_body = {
                        "exchange": exchange,
                        "pair": pair,
                        "direction": direction,
                        "time": ts,
                        "amount": amount,
                        "price": price
                    }

                    await trades.send(value=json_body)

@app.agent(zbTrades)
async def zbtrades(msgs):
    async for msg in msgs:
        if 'data' in msg:
            exchange = 'zb'
            pair = msg['channel'].split('_')[0]
            for submsg in msg['data']:
                amount = float(submsg['amount'])
                price = float(submsg['price'])
                direction = normalizeDirectionField[submsg['type']]
                ts = submsg['date'] * 1000

                json_body = {
                    "exchange": exchange,
                    "pair": pair,
                    "direction": direction,
                    "time": ts,
                    "amount": amount,
                    "price": price
                }

                await trades.send(value=json_body)

if __name__ == '__main__':
    app.main()
