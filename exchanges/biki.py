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
        msgs = base.json.loads(decompressedMsg)
        try:
            pingId = msgs['ping']
            pongMsg = base.json.dumps({"pong": pingId})
            self.sendMessage(pongMsg.encode('utf8')) # Pong server with given ID

        except:
            for msg in msgs['tick']['data']:
                exchange = self.__class__.__name__
                amount = float(msg['vol'])
                price = float(msg['price'])
                direction = self.normalizeDirectionField[msg['side'].lower()]
                ts = msg['ts']//1000

                self.insertData(exchange, amount, price, direction, ts)

    def start():
        base.createConnection("wss://ws.biki.com/kline-api/ws", 443, Biki)
