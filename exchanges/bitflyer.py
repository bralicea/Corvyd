import base


class Bitflyer(base.Base):

    def onOpen(self):
        for instrument in base.instruments.instruments['bitflyer']:
            msgParams = {"method": "subscribe", "params": {"channel": 'lightning_executions_{}'.format(instrument)}}
            subscription = base.json.dumps(msgParams)
            self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('bitflyerTrades', payload)


base.createConnection("wss://ws.lightstream.bitflyer.com/json-rpc", 443, Bitflyer)
