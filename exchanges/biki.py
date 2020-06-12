import base

class Biki(base.Base):

    def onOpen(self):
        params = {
            "event": "sub",
            "params": {
                "channel": "market_btcusdt_trade_ticker",
                "cb_id": "Customer"
            }
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('bikiTrades', payload)


def start():
    base.createConnection("wss://ws.biki.com/kline-api/ws", 443, Biki)


start()
