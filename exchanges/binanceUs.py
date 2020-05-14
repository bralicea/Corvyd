import base


class BinanceUs(base.Base):

    def onMessage(self, payload, isBinary):
        self.producer.send('binanceUsTrades', payload)


def start():
    base.createConnection("wss://stream.binance.us:9443/stream?streams=btcusdt@trade", 9443, BinanceUs)


start()
