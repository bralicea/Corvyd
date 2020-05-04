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
        msg = base.json.loads(decompressedMsg)
        try:
            exchange = self.__class__.__name__
            amount = float(msg['data'][0]['size'])
            price = float(msg['data'][0]['price'])
            direction = self.normalizeDirectionField[msg['data'][0]['side']]
            ts = msg['data'][0]['timestamp']
            dt = base.datetime.strptime(ts.split('.')[0],"%Y-%m-%dT%H:%M:%S")
            ts = int((dt - base.datetime.utcfromtimestamp(0)).total_seconds())

            self.insertData(exchange, amount, price, direction, ts)

        except:
            pass

    def start():
        base.createConnection("wss://real.okex.com:8443/ws/v3", 8443, Okex)