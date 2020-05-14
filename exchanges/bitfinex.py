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
        self.producer.send('bitfinexTrades', payload)


def start():
    base.createConnection("wss://api-pub.bitfinex.com/ws/2", 443, Bitfinex)

start()