import base

class Bitstamp(base.Base):

    def onOpen(self):
        params = {
            'event': 'bts:subscribe',
            'data': {
                'channel': 'live_trades_btcusd'
            }
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        strMsg = payload.decode('utf8')
        msg = base.json.loads(strMsg)['data']
        try:
            exchange = self.__class__.__name__
            amount = msg['amount']
            price = msg['price']
            if msg['type'] == 0:
                direction = '1'
            else:
                direction = '0'
            ts = msg['timestamp']

            self.insertData(exchange, amount, price, direction, ts)

        except:
            pass

    def start():
        base.createConnection("wss://ws.bitstamp.net", 443, Bitstamp)
