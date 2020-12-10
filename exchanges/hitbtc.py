# https://api.hitbtc.com/#socket-market-data
import base


class Hitbtc(base.Base):

    def onOpen(self):
        for instrument in base.instruments.instruments['hitbtc']:
            params = {
              "method": "subscribeTrades",
              "params": {
                "symbol": "{}".format(instrument),
                "limit": 1
              },
              "id": 123
            }
            subscription = base.json.dumps(params)
            self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('hitbtcTrades', payload)


class HitbtcOB(Hitbtc):

    def onOpen(self):
        for instrument in base.instruments.instruments['hitbtc']:
            params = {
              "method": "subscribeOrderbook",
              "params": {
                "symbol": "{}".format(instrument)
              },
              "id": 123
            }
            subscription = base.json.dumps(params)
            self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('hitbtcOrderBooks', payload)


base.createConnection("wss://api.hitbtc.com/api/2/ws", 443, Hitbtc)
base.createConnection("wss://api.hitbtc.com/api/2/ws", 443, HitbtcOB)
