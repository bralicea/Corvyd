# https://docs.gemini.com/websocket-api/
import base
import instruments


class Gemini(base.Base):

    def onOpen(self):
        params = '{"type": "subscribe","subscriptions":[{"name":"l2","symbols":' + (instruments.instruments['gemini']) + '}]}'
        self.sendMessage(params.encode('utf8'))

    def onMessage(self, payload, isBinary):
        msg = base.json.loads(payload.decode('utf-8'))
        if msg['type'] == 'trade':
            self.producer.send('geminiTrades', base.json.dumps(msg).encode('utf-8'))

        elif msg['type'] == 'l2_updates':
            self.producer.send('geminiOrderBooks', base.json.dumps(msg).encode('utf-8'))


base.createConnection("wss://api.gemini.com/v2/marketdata", 443, Gemini)
