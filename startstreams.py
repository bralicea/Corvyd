import base
import bitstamp
import binance
import binanceUs
import bitfinex
import bitforex
import biki
import zb
import gate
import coinex
import okex

if __name__ == '__main__':
    bitstamp.start()
    binance.start()
    binanceUs.start()
    bitfinex.start()
    biki.start()
    zb.start()
    gate.start()
    coinex.start()
    okex.start()
    bitforex.start()

    base.reactor.run()
