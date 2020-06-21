import base

class Coinbase(base.Base):
    def onOpen(self):
        params = {
            "type": "subscribe",
            "channels": [{"name": "ticker", "product_ids": ["LTC-USD"]}]
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

base.createConnection("wss://ws-feed.pro.coinbase.com", 443, Coinbase)

base.reactor.run()