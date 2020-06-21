import base


class CoinEx(base.Base):

    def onOpen(self):
        params = {
          "method": "deals.subscribe",
          "params": base.instruments.instruments['coinex'],
          "id": 16
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('coinexTrades', payload)


base.createConnection("wss://socket.coinex.com/", 443, CoinEx)
