# https://docs.pro.coinbase.com/#websocket-feed
import base
import sys, os


class Coinbase(base.Base):
    def onOpen(self):
        for instrument in base.instruments.instruments['coinbase']:
            params = {
                "type": "subscribe",
                "channels": [{"name": "matches", "product_ids": [instrument]}]
            }
            subscription = base.json.dumps(params)
            self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('coinbaseTrades', payload)


class CoinbaseOB(Coinbase):

    def onOpen(self):
        for instrument in base.instruments.instruments['coinbase']:
            params = {
                "type": "subscribe",
                "channels": [{"name": "level2", "product_ids": [instrument]}]
            }
            subscription = base.json.dumps(params)
            self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('coinbaseOrderBooks', payload)


base.createConnection("wss://ws-feed.pro.coinbase.com", 443, Coinbase)
base.createConnection("wss://ws-feed.pro.coinbase.com", 443, CoinbaseOB)
