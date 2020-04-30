import base

class Binance(base.Base):

    def onMessage(self, payload, isBinary):
        strMsg = payload.decode('utf8')
        msg = base.json.loads(strMsg)['data']

        exchange = self.__class__.__name__
        amount = msg['q']
        price = msg['p']
        direction = self.normalizeDirectionField[msg['m']]
        ts = msg['T']//1000

        self.insertData(exchange, amount, price, direction, ts)

def start():
    base.createConnection("wss://stream.binance.com:9443/stream?streams=dogeusdt@trade/dogebtc@trade/btcusdt@trade", 9443, Binance)
