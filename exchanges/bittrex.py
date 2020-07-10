"""import base


class Bittrex(base.Base):

    def onOpen(self):
            params = {"H":"c3","M":"Subscribe","A":[["heartbeat","ticker_BTC-USD"]],"I":1}
            subscription = base.json.dumps(params)
            self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        print(payload)
        #self.producer.send('kucoinTrades', payload)


base.createConnection("https://socket.bittrex.com/signalr", 443, Bittrex)
base.reactor.run()"""

from __future__ import print_function
from time import sleep
from bittrex_websocket import BittrexSocket, BittrexMethods

def main():
    class MySocket(BittrexSocket):

        def on_public(self, msg):
            # Create entry for the ticker in the trade_history dict
            if msg['invoke_type'] == BittrexMethods.SUBSCRIBE_TO_EXCHANGE_DELTAS:
                print(msg)


    # Create container
    trade_history = {}
    # Create the socket instance
    ws = MySocket()
    # Enable logging
    ws.enable_log()
    # Define tickers
    tickers = ['USD-BTC']
    # Subscribe to trade fills
    ws.subscribe_to_exchange_deltas(tickers)

    while len(set(tickers) - set(trade_history)) > 0:
        sleep(1)
    else:
        for ticker in trade_history.keys():
            print('Printing {} trade history.'.format(ticker))
            for trade in trade_history[ticker]:
                print(trade)
        ws.disconnect()
        sleep(10)

if __name__ == "__main__":
    main()