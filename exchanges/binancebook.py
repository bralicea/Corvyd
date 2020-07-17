import base


class Binance(base.Base):

    def onMessage(self, payload, isBinary):
        self.producer.send('binanceOrderBook1', payload)


def start():
    base.createConnection("wss://stream.binance.com:9443/stream?streams=btcusdt@depth20", 9443, Binance)


start()
