# https://docs.kraken.com/websockets/
import base


class Kraken(base.Base):
    def onOpen(self):
        params = {
          "event": "subscribe",
          "pair": base.instruments.instruments['kraken'],
          "subscription": {
            "name": "trade"
          }
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('krakenTrades', payload)


class KrakenOB(Kraken):
    def onOpen(self):
        params = {
          "event": "subscribe",
          "pair": base.instruments.instruments['kraken'],
          "subscription": {
            "name": "book",
            "depth": 25
          }
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('krakenOrderBooks', payload)


base.createConnection("wss://ws.kraken.com", 443, Kraken)
base.createConnection("wss://ws.kraken.com", 443, KrakenOB)
