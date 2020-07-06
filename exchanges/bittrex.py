import base


class Bittrex(base.Base):

    def onOpen(self):
            params = {"H":"c3","M":"Subscribe","A":[["heartbeat","ticker_BTC-USD"]],"I":1}
            subscription = base.json.dumps(params)
            self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        print(payload)
        #self.producer.send('kucoinTrades', payload)


base.createConnection("wss://socket.bittrex.com/signalr", 443, Bittrex)
base.reactor.run()