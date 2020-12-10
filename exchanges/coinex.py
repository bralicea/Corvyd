# https://github.com/coinexcom/coinex_exchange_api/wiki
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


class CoinExOB(CoinEx):

    def onOpen(self):
        params = {
            "method": "depth.subscribe_multi",
            "params": [[pair, 20, "0"] for pair in base.instruments.instruments['coinex']],
            "id": 15
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('coinexOrderBooks', payload)


base.createConnection("wss://socket.coinex.com/", 443, CoinEx)
base.createConnection("wss://socket.coinex.com/", 443, CoinExOB)
