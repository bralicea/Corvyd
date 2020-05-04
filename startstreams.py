import base
from exchanges import bitstamp, binance, binanceUs, bitfinex, bitforex, biki, zb, gate, coinex, okex, bitmex

if __name__ == '__main__':
    bitstamp.Bitstamp.start()
    binance.Binance.start()
    binanceUs.BinanceUs.start()
    bitfinex.Bitfinex.start()
    biki.Biki.start()
    zb.Zb.start()
    gate.Gate.start()
    coinex.CoinEx.start()
    okex.Okex.start()
    bitforex.Bitforex.start()
    bitmex.Bitmex.start()


    base.reactor.run()
