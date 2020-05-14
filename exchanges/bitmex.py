import base

class Bitmex(base.Base):

    def onMessage(self, payload, isBinary):
        self.producer.send('bitmexTrades', payload)


def start():
    base.createConnection("wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD", 443, Bitmex)


start()
