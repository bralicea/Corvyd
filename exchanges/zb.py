import base


class Zb(base.Base):

    def onOpen(self):
        params = {
            "event": "addChannel",
            "channel": "btcusdt_trades"
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('zbTrades', payload)


def start():
    base.createConnection("wss://api.zb.com:9999/websocket", 443, Zb)


start()
