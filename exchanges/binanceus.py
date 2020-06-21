import base


class BinanceUs(base.Base):

    def onMessage(self, payload, isBinary):
        self.producer.send('binanceUsTrades', payload)


base.createConnection("wss://stream.binance.us:9443/stream?streams={}".format(base.instruments.instruments['binanceus']), 9443, BinanceUs)
