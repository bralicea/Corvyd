# https://www.okex.com/docs/en/#README
import base


class Okex(base.Base):

    def onOpen(self):
        params = {
            "op": "subscribe",
            "args": base.instruments.instruments['okex']
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('okexTrades', payload)


class OkexOB(base.Base):
    ob = {}
    def onOpen(self):
        params = {
            "op": "subscribe",
            "args": ["spot/depth:ETH-USDT"]
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        msg = base.json.loads(base.zlib.decompress(payload, -base.zlib.MAX_WBITS | 32))
        print(msg)
        #self.producer.send('okexTrades', payload)


#base.createConnection("wss://real.okex.com:8443/ws/v3", 8443, Okex)
base.createConnection("wss://real.okex.com:8443/ws/v3", 8443, OkexOB)

base.reactor.run()