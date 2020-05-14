import base


class CoinEx(base.Base):

    def onOpen(self):
        params = {
          "method": "deals.subscribe",
          "params": [
            "BTCUSDT"
          ],
          "id": 16
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('coinexTrades', payload)


def start():
    base.createConnection("wss://socket.coinex.com/", 443, CoinEx)


start()
