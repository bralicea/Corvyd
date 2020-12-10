# https://docs.deribit.com/?python#trades-instrument_name-interval
import base


class Deribit(base.Base):

    def onOpen(self):
        params = {
            "jsonrpc": "2.0",
             "method": "public/subscribe",
             "id": 42,
             "params": {
                "channels": ["trades.BTC-PERPETUAL.raw", "trades.ETH-PERPETUAL.raw"]}
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('deribitTrades', payload)


class DeribitOB(Deribit):

    def onOpen(self):
        params = {
            "jsonrpc": "2.0",
             "method": "public/subscribe",
             "id": 42,
             "params": {
                "channels": ["book.BTC-PERPETUAL.100ms", "book.ETH-PERPETUAL.100ms"]}
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('deribitOrderBooks', payload)


base.createConnection("wss://test.deribit.com/ws/api/v2", 443, Deribit)
base.createConnection("wss://test.deribit.com/ws/api/v2", 443, DeribitOB)
