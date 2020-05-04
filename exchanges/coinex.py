import base

class CoinEx(base.Base):

    def onOpen(self):
        params = {
          "method":"deals.subscribe",
          "params":[
            "BTCUSDT"
          ],
          "id":16
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        msgList = base.json.loads(payload.decode('utf8'))
        try:
            for msg in msgList['params'][1]:
                exchange = self.__class__.__name__
                amount = msg['amount']
                price = msg['price']
                direction = self.normalizeDirectionField[msg['type']]
                ts = int(msg['time']//1)

                self.insertData(exchange, amount, price, direction, ts)

        except:
            pass

    def start():
        base.createConnection("wss://socket.coinex.com/", 443, CoinEx)
