import faust
import psycopg2
from datetime import datetime
import zlib
import json

# Connect to database
conn = psycopg2.connect(database="postgres", user='bralicea', password='Roflmao24!', host='database-1.c8pnybnwk3oh.us-east-2.rds.amazonaws.com', port='5432')
conn.autocommit = True
cursor = conn.cursor()

# Dictionary meant to normalize data type for the 'direction' field in database
normalizeDirectionField = {True: '1', '1': '1', 'buy': '1',
                           False: '0', '2': '0', 'sell': '0'}


# Insert data into database
def insertData(exchange, amount, price, direction, ts):
    sql = '''INSERT into trades (exchange, amount, price, direction, ts) values(%s, %s, %s, %s, %s)''';
    data = (exchange, amount, price, direction, ts)
    cursor.execute(sql, data)
    conn.commit()


# Start Faust
app = faust.App('hello-app', broker='kafka://localhost')
binanceTrades = app.topic('binanceTrades')
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


@app.agent(binanceTrades)
async def binancetrades(msgs):
    async for msg in msgs:
        msg = msg['data']
        exchange = 'Binance'
        pair = msg['s']
        amount = msg['q']
        price = msg['p']
        direction = normalizeDirectionField[msg['m']]
        ts = msg['T'] // 1000
        print(exchange, pair, amount, price, direction, ts)

@app.agent(binanceUsTrades)
async def binanceustrades(msgs):
    async for msg in msgs:
        msg = msg['data']
        exchange = 'BinanceUs'
        pair = msg['s']
        amount = msg['q']
        price = msg['p']
        direction = normalizeDirectionField[msg['m']]
        ts = msg['T'] // 1000
        print(exchange, pair, amount, price, direction, ts)

@app.agent(bikiTrades)
async def bikitrades(msgs):
    async for msg in msgs:
        msg = json.loads(zlib.decompress(msg, zlib.MAX_WBITS | 32))
        if 'event_rep' in msg:
            exchange = 'Biki'
            pair = msg['channel'].split('_')[1]
            amount = float(msg['tick']['data'][0]['vol'])
            price = float(msg['tick']['data'][0]['price'])
            direction = normalizeDirectionField[msg['tick']['data'][0]['side'].lower()]
            ts = msg['tick']['data'][0]['ts'] // 1000
            print(exchange, pair, amount, price, direction, ts)

@app.agent(bitfinexTrades)
async def bitfinextrades(msgs):
    async for msg in msgs:
        if len(msg) == 3:
            exchange = 'Bitfinex'
            pair = 'btcusd'
            amount = msg[2][2]
            price = msg[2][3]
            if amount >= 0:
                direction = "1"
            else:
                direction = "0"
            ts = msg[2][1] // 1000
            print(exchange, pair, abs(amount), price, direction, ts)

@app.agent(bitforexTrades)
async def bitforextrades(msgs):
    async for msg in msgs:
        exchange = 'Bitforex'
        pairFormat = msg['param']['businessType'].split('-')
        pair = pairFormat[2] + pairFormat[1]
        amount = msg['data'][0]['amount']
        price = msg['data'][0]['price']
        direction = normalizeDirectionField[str(msg['data'][0]['direction'])]
        ts = msg['data'][0]['time'] // 1000
        print(exchange, pair, amount, price, direction, ts)

@app.agent(bitmexTrades)
async def bitmextrades(msgs):
    async for msg in msgs:
        if 'data' in msg:
            msg = msg['data'][0]
            exchange = 'Bitmex'
            pair = msg['symbol'].replace('XBT', 'btc').lower()
            amount = msg['size']
            price = msg['price']
            direction = normalizeDirectionField[msg['side'].lower()]
            dt = datetime.strptime(msg['timestamp'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds())
            print(exchange, pair, amount, price, direction, ts)

@app.agent(bitstampTrades)
async def bitstamptrades(msgs):
    async for msg in msgs:
        if 'data' in msg:
            exchange = 'Bitstamp'
            pair = msg['channel'].split('_')[2]
            amount = msg['data']['amount']
            price = msg['data']['price']
            if msg['data']['type'] == 0:
                direction = '1'
            else:
                direction = '0'
            ts = msg['data']['timestamp']
            print(exchange, pair, amount, price, direction, ts)

@app.agent(coinexTrades)
async def coinextrades(msgs):
    async for msg in msgs:
        if 'method' in msg:
            exchange = 'Coinex'
            pair = msg['params'][0].lower()
            for ms in msg['params'][1]:
                amount = float(ms['amount'])
                price = float(ms['price'])
                direction = normalizeDirectionField[ms['type']]
                ts = int(ms['time'] // 1)
                print(exchange, pair, amount, price, direction, ts)

@app.agent(gateTrades)
async def gatetrades(msgs):
    async for msg in msgs:
        if 'method' in msg:
            exchange = 'Gate'
            pairFormat = msg['params'][0].split('_')
            pair = (pairFormat[0] + pairFormat[1]).lower()
            for submsg in msg['params'][1]:
                amount = float(submsg['amount'])
                price = float(submsg['price'])
                direction = normalizeDirectionField[submsg['type']]
                ts = int(submsg['time'] // 1)
                print(exchange, pair, amount, price, direction, ts)

@app.agent(okexTrades)
async def okextrades(msgs):
    async for msg in msgs:
        msg = json.loads(zlib.decompress(msg, -zlib.MAX_WBITS | 32))
        if 'table' in msg:
            msg = msg['data'][0]
            exchange = 'Okex'
            pairFormat = msg['instrument_id'].split('-')
            pair = (pairFormat[0] + pairFormat[1]).lower()
            amount = float(msg['size'])
            price = float(msg['price'])
            direction = normalizeDirectionField[msg['side']]
            dt = datetime.strptime(msg['timestamp'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = int((dt - datetime.utcfromtimestamp(0)).total_seconds())
            print(exchange, pair, amount, price, direction, ts)

@app.agent(zbTrades)
async def zbtrades(msgs):
    async for msg in msgs:
        exchange = 'Zb'
        pair = msg['channel'].split('_')[0]
        for submsg in msg['data']:
            amount = submsg['amount']
            price = submsg['price']
            direction = normalizeDirectionField[submsg['type']]
            ts = submsg['date']
            print(exchange, pair, amount, price, direction, ts)

if __name__ == '__main__':
    app.main()