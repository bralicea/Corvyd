import base

class Zb(base.Base):

    lastDate = ""

    def onOpen(self):
        params = {
            "event": "addChannel",
            "channel": "btcusdt_trades"
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        msgList = base.json.loads(payload.decode('utf8'))["data"]
        for msg in msgList[::-1]:
            if msg["date"] == self.lastDate: # Loop until lastDate to avoid repeats
                self.lastDate = msg["date"]
                break

            if msg["tid"] == msgList[0]["tid"]: # Last message in msgList[::-1] reached
                self.lastDate = msgList[-1]["date"]

            else:
                exchange = self.__class__.__name__
                amount = msg['amount']
                price = msg['price']
                direction = self.normalizeDirectionField[msg['type']]
                ts = msg['date']

                self.insertData(exchange, amount, price, direction, ts)

    def start():
        base.createConnection("wss://api.zb.com:9999/websocket", 443, Zb)
