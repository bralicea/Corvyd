import base

class Bitstamp(base.Base):

    def onOpen(self):
        params = {
            'event': 'bts:subscribe',
            'data': {
                'channel': 'live_trades_btcusd'
            }
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('bitstampTrades', payload)


def start():
    base.createConnection("wss://ws.bitstamp.net", 443, Bitstamp)


start()
