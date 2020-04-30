import base

class BinanceUs(base.Base):

    def onMessage(self, payload, isBinary):
        strMsg = payload.decode('utf8')
        msg = base.json.loads(strMsg)['data']

        exchange = self.__class__.__name__
        amount = msg['q']
        price = msg['p']
        direction = self.normalizeDirectionField[msg['m']]
        ts = msg['T']

        self.insertData(exchange, amount, price, direction, ts)

def start():
    base.createConnection("wss://stream.binance.us:9443/stream?streams=dogeusdt@trade/dogebtc@trade/btcusdt@trade", 9443, BinanceUs)
