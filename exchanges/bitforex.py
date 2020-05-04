import base

class Bitforex(base.Base):

    now = base.time.time()
    def onOpen(self):
        params = [{
            "type": "subHq",
            "event": "trade",
            "param": {
                "businessType": "coin-usdt-btc",
                "size": 1
            }
        }]
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        try:
            msg = base.json.loads(payload.decode('utf8'))['data'][0]

            exchange = self.__class__.__name__
            amount = msg['amount']
            price = msg['price']
            direction = self.normalizeDirectionField[str(msg['direction'])]
            ts = msg['time']//1000

            self.insertData(exchange, amount, price, direction, ts)

            # Send ping message to server
            if base.time.time() - self.now >= 60:
                self.sendMessage("ping_p".encode())
                self.now = base.time.time()

        except:
            pass

    def start():
        base.createConnection("wss://www.bitforex.com/mkapi/coinGroup1/ws", 443, Bitforex)