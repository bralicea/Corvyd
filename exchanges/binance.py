import base


class Binance(base.Base):

    def onMessage(self, payload, isBinary):
        self.producer.send('binanceTrades', payload)


base.createConnection("wss://stream.binance.com:9443/stream?streams={}".format(base.instruments.instruments['binance']), 9443, Binance)
