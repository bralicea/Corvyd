import base


class Bitmex(base.Base):

    def onMessage(self, payload, isBinary):
        self.producer.send('bitmexTrades', payload)


base.createConnection("wss://www.bitmex.com/realtime?subscribe={}".format(base.instruments.instruments['bitmex']), 443, Bitmex)
