import faust
import psycopg2
from datetime import datetime
import zlib
import time
import json
import instruments

# Connect to database
conn = psycopg2.connect(database="postgres", user='bralicea', password='Roflmao24!', host='database-1.c8pnybnwk3oh.us-east-2.rds.amazonaws.com', port='5432')
conn.autocommit = True
cursor = conn.cursor()

# Dictionary meant to normalize data type for the 'direction' field in database
normalizeDirectionField = {False: 'buy', '1': 'buy', 'buy': 'buy',
                           True: 'sell', '2': 'sell', 'sell': 'sell'}


# Start Faust
app = faust.App('hello-app', broker='kafka://localhost')
ingestData = app.topic('ingestData')
binanceTrades = app.topic('binanceTrades')
binanceOrderBook = app.topic('binanceOrderBook1')
binanceUsTrades = app.topic('binanceUsTrades')
bikiTrades = app.topic('bikiTrades', value_serializer='raw')
bitfinexTrades = app.topic('bitfinexTrades')
bitforexTrades = app.topic('bitforexTrades')
bitmexTrades = app.topic('bitmexTrades')
bitstampTrades = app.topic('bitstampTrades')
coinexTrades = app.topic('coinexTrades')
gateTrades = app.topic('gateTrades')
okexTrades = app.topic('okexTrades', value_serializer='raw')
zbTrades = app.topic('zbTrades')

# Insert data into database
@app.agent(ingestData)
async def ingestdata(dataList):
    async for data in dataList:
        pass
        #sql = '''INSERT into trades (exchange, amount, price, direction, ts, pair) values(%s, %s, %s, %s, %s, %s)''';
        #data = (data[0], data[1], data[2], data[3], data[4], data[5])
        #cursor.execute(sql, data)
        #conn.commit()

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
        print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})
        #await ingestdata.send(value=[exchange, amount, price, direction, ts, pair])

@app.agent(binanceOrderBook)
async def binanceorderbook(msgs):
    async for msg in msgs:
        msg = msg['data']
        msg['timestamp'] = int(time.time()//1)
        await ingestdata.send(value=msg)

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
        print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})

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
            print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})

@app.agent(bitfinexTrades)
async def bitfinextrades(msgs):
    bitfinexId = {}
    async for msg in msgs:
        if 'event' in msg and msg['event'] == 'subscribed':
            bitfinexId[msg['chanId']] = msg['symbol']

        elif len(msg) == 3:
            exchange = 'bitfinex'
            pair = bitfinexId[msg[0]]
            amount = msg[2][2]
            price = msg[2][3]
            if amount >= 0:
                direction = "buy"
            else:
                direction = "sell"
            ts = msg[2][1] // 1000
            print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})

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
        print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})

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
            print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})

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
            print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})

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
                print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})

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
                print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})

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
            print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})

@app.agent(zbTrades)
async def zbtrades(msgs):
    async for msg in msgs:
        exchange = 'zb'
        pair = msg['channel'].split('_')[0]
        for submsg in msg['data']:
            amount = submsg['amount']
            price = submsg['price']
            direction = normalizeDirectionField[submsg['type']]
            ts = submsg['date']
            print({'exchange': exchange, 'pair': pair, 'amount': amount, 'price': price, 'direction': direction, 'ts': ts})

if __name__ == '__main__':
    app.main()
