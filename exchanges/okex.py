import base


class Okex(base.Base):

    def onOpen(self):
        params = {
            "op": "subscribe",
            "args": ["spot/trade:BTC-USDT"]
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        decompressedMsg = base.zlib.decompress(payload, -base.zlib.MAX_WBITS|32) # Decompress binary data
        self.producer.send('okexTrades0', decompressedMsg)


def start():
    base.createConnection("wss://real.okex.com:8443/ws/v3", 8443, Okex)


start()
