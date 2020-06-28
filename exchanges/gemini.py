import base


class Gemini(base.Base):

    def onMessage(self, payload, isBinary):
        print(payload)
        #self.producer.send('geminiTrades', payload)


base.createConnection("wss://api.gemini.com/v1/marketdata/ethusd?trades=true", 443, Gemini)

base.reactor.run()