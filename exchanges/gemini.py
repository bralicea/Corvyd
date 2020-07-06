import base


class GeminiBTC(base.Base):

    def onMessage(self, payload, isBinary):
        payload = base.json.loads(payload.decode('utf-8'))
        payload["pair"] = 'btcusd'
        self.producer.send('geminiTrades', base.json.dumps(payload).encode('utf-8'))

class GeminiETH(base.Base):

    def onMessage(self, payload, isBinary):
        payload = base.json.loads(payload.decode('utf-8'))
        payload["pair"] = 'ethusd'
        self.producer.send('geminiTrades', base.json.dumps(payload).encode('utf-8'))


base.createConnection("wss://api.gemini.com/v1/marketdata/btcusd?trades=true", 443, GeminiBTC)
base.createConnection("wss://api.gemini.com/v1/marketdata/ethusd?trades=true", 443, GeminiETH)
