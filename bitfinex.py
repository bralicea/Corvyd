import base

class Bitfinex(base.Base):

    def onOpen(self):
        params = {
            'event': 'subscribe',
            'channel': 'trades',
    		'symbol': 'tBTCUSD'
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        strMsg = payload.decode('utf8')
        msg = base.ast.literal_eval(strMsg)
        try:
            exchange = self.__class__.__name__
            amount = msg[2][2]
            price = msg[2][3]
            if amount >= 0:
                direction = "1"
            else:
                direction = "0"
            ts = msg[2][1]//1000

            self.insertData(exchange, amount, price, direction, ts)

        except:
            pass

def start():
    base.createConnection("wss://api-pub.bitfinex.com/ws/2", 443, Bitfinex)
