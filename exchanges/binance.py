import base


class Binance(base.Base):

    def onMessage(self, payload, isBinary):
        self.producer.send('binanceTrades', payload)


def start():
    base.createConnection("wss://stream.binance.com:9443/stream?streams=btcusdt@trade", 9443, Binance)


start()