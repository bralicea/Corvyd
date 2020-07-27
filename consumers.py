import faust
from datetime import datetime
import zlib
import time
import json
import base64
import instruments

# Dictionary meant to normalize data type for the 'direction' field in database
normalizeDirectionField = {False: 'buy', '1': 'buy', 'buy': 'buy', 'b': 'buy', 'bid': 'buy', 1: 'buy',
                           True: 'sell', '2': 'sell', 'sell': 'sell', 's': 'sell', 'ask': 'sell', 2: 'sell', 0: 'sell'}

# Start Faust
app = faust.App('hello-app', broker='ec2-18-208-126-104.compute-1.amazonaws.com:9092')
Trades = app.topic('Trades')
OrderBooks = app.topic('OrderBooks')
biboxTrades = app.topic('biboxTrades')
binanceTrades = app.topic('binanceTrades')
binanceOrderBook = app.topic('binanceOrderBook1')
binanceUsTrades = app.topic('binanceUsTrades')
bikiTrades = app.topic('bikiTrades', value_serializer='raw')
bitfinexTrades = app.topic('bitfinexTrades')
bitflyerTrades = app.topic('bitflyerTrades')
bitforexTrades = app.topic('bitforexTrades')
bitmexTrades = app.topic('bitmexTrades')
bitstampTrades = app.topic('bitstampTrades'); bittrexTrades = app.topic('bittrexTrades', value_serializer='raw')
coinbaseTrades = app.topic('coinbaseTrades')
coinexTrades = app.topic('coinexTrades')
gateTrades = app.topic('gateTrades')
geminiTrades = app.topic('geminiTrades')
hitbtcTrades = app.topic('hitbtcTrades')
huobiTrades = app.topic('huobiTrades', value_serializer='raw')
krakenTrades = app.topic('krakenTrades')
kucoinTrades = app.topic('kucoinTrades')
okexTrades = app.topic('okexTrades', value_serializer='raw')
phemexTrades = app.topic('phemexTrades')
poloniexTrades = app.topic('poloniexTrades')
zbTrades = app.topic('zbTrades')

# Ingest trade data from Druid
@app.agent(Trades)
async def trades(dataList):
    async for data in dataList:
        pass

# Ingest order book data from Druid
@app.agent(OrderBooks)
async def orderbooks(dataList):
    async for data in dataList:
        pass

@app.agent(biboxTrades)
async def biboxtrades(msgs):
    async for msg in msgs:
        if isinstance(msg, list):
            # decompress, decode, then jsonify msg
            msg = json.loads((zlib.decompress(base64.b64decode(msg[0]['data']), zlib.MAX_WBITS | 32)).decode('utf-8'))
            for submsg in msg:
                exchange = 'bibox'
                pair = submsg['pair'].replace('_', '').lower()
                amount = submsg['amount']
                price = submsg['price']
                direction = normalizeDirectionField[submsg['side']]
                ts = submsg['time'] // 1000
                await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(binanceTrades)
async def binancetrades(msgs):
    async for msg in msgs:
        msg = msg['data']
        exchange = 'binance'
        pair = msg['s'].lower()
        amount = msg['q']
        price = msg['p']
        direction = normalizeDirectionField[msg['m']]
        ts = msg['T'] // 1000
        await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(binanceOrderBook)
async def binanceorderbook(msgs):
    async for msg in msgs:
        dict = {'exchange': 'binance', 'pair': msg['stream'].split('@')[0], 'bids': msg['data']['bids'], 'asks': msg['data']['asks'], 'time': int(time.time()//1)}
        await orderbooks.send(value=dict)

@app.agent(binanceUsTrades)
async def binanceustrades(msgs):
    async for msg in msgs:
        msg = msg['data']
        exchange = 'binanceus'
        pair = msg['s'].lower()
        amount = msg['q']
        price = msg['p']
        direction = normalizeDirectionField[msg['m']]
        ts = msg['T'] // 1000
        await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

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
            ts = msg['tick']['data'][0]['ts'] // 1000
            await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(bitfinexTrades)
async def bitfinextrades(msgs):
    # Bitfinex generates new pair IDs every connection
    bitfinexId = {}
    async for msg in msgs:
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
            amount = abs(msg[2][2])
            price = msg[2][3]
            if amount >= 0:
                direction = "buy"
            else:
                direction = "sell"
            ts = msg[2][1] // 1000
            await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

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
            ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds())
            await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(bitforexTrades)
async def bitforextrades(msgs):
    async for msg in msgs:
        exchange = 'bitforex'
        pairFormat = msg['param']['businessType'].split('-')
        pair = pairFormat[2] + pairFormat[1]
        amount = msg['data'][0]['amount']
        price = msg['data'][0]['price']
        direction = normalizeDirectionField[str(msg['data'][0]['direction'])]
        ts = msg['data'][0]['time'] // 1000
        await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

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
            amount = msg['size']
            price = msg['price']
            direction = normalizeDirectionField[msg['side'].lower()]
            dt = datetime.strptime(msg['timestamp'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds())
            await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

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
            ts = msg['data']['timestamp']
            await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

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
                ts = data['T'] // 1000
                await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

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
                ts = int(ms['time'] // 1)
                await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(coinbaseTrades)
async def coinbasetrades(msgs):
    async for msg in msgs:
        if msg['type'] == 'match':
            exchange = 'coinbase'
            pair = msg['product_id'].lower()
            amount = msg['size']
            price = msg['price']
            direction = msg['side']
            dt = datetime.strptime(msg['time'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds())
            await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

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
                ts = int(submsg['time'] // 1)
                await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(geminiTrades)
async def geminitrades(msgs):
    async for msg in msgs:
        if msg['socket_sequence'] != 0:
            for data in msg['events']:
                exchange = 'gemini'
                pair = msg['pair']
                amount = data['amount']
                price = data['price']
                direction = normalizeDirectionField[data['makerSide']]
                ts = msg['timestamp']
                await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(hitbtcTrades)
async def hitbtctrades(msgs):
    async for msg in msgs:
        if 'params' in msg:
            for data in msg['params']['data']:
                exchange = 'hitbtc'
                pair = msg['params']['symbol'].lower()
                amount = data['quantity']
                price = data['price']
                direction = data['side']
                dt = datetime.strptime(data['timestamp'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
                ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds())
                await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

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
                ts = data['ts'] // 1000
                await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(krakenTrades)
async def krakentrades(msgs):
    async for msg in msgs:
        if len(msg) == 4 and isinstance(msg, list):
            for data in msg[1]:
                exchange = 'kraken'
                if 'XBT' in msg[3]:
                    msg[3] = msg[3].replace('XBT', 'BTC')
                pair = msg[3].replace('/', '').lower()
                amount = data[1]
                price = data[0]
                direction = normalizeDirectionField[data[3]]
                ts = int(float(data[2])//1)
                await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(kucoinTrades)
async def kucointrades(msgs):
    async for msg in msgs:
        if 'data' in msg and 'sequence' in msg['data']:
            msg = msg['data']
            exchange = 'kucoin'
            pair = msg['symbol'].replace('-', '').lower()
            amount = msg['size']
            price = msg['price']
            direction = msg['side']
            ts = int(msg['time'])//1000
            await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

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
            ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds())
            await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

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
                ts = data[0]//10000
                await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(poloniexTrades)
async def poloniextrades(msgs):
    async for msg in msgs:
        if len(msg) >= 3:
            for submsg in msg[2]:
                if submsg[0] == 't':
                    exchange = 'poloniex'
                    pairFormat = (instruments.poloniexId[msg[0]]).split('_')
                    pair = (pairFormat[1] + pairFormat[0]).lower()
                    amount = submsg[4]
                    price = submsg[3]
                    direction = normalizeDirectionField[submsg[2]]
                    ts = submsg[5]
                    await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

@app.agent(zbTrades)
async def zbtrades(msgs):
    async for msg in msgs:
        if 'data' in msg:
            exchange = 'zb'
            pair = msg['channel'].split('_')[0]
            for submsg in msg['data']:
                amount = submsg['amount']
                price = submsg['price']
                direction = normalizeDirectionField[submsg['type']]
                ts = submsg['date']
                await trades.send(value={'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'time': ts})

if __name__ == '__main__':
    app.main()
