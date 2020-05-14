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
        decompressedMsg = base.zlib.decompress(payload, base.zlib.MAX_WBITS|32) # Decompress binary data
        msg = base.json.loads(decompressedMsg)
        if 'ping' in msg:
            pingId = msg['ping']
            pongMsg = base.json.dumps({"pong": pingId})
            self.sendMessage(pongMsg.encode('utf8'))  # Pong server with given ID

        else:
            self.producer.send('bikiTrades', decompressedMsg)


def start():
    base.createConnection("wss://ws.biki.com/kline-api/ws", 443, Biki)


start()
