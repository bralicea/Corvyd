import base

class Zb(base.Base):

    def onOpen(self):
        params = {
            "event": "addChannel",
            "channel": "btcusdt_trades"
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        msgList = base.json.loads(payload.decode('utf8'))["data"]
        global lastDate
        for msg in msgList[::-1]:
            if msg["date"] == lastDate: # Loop until lastDate to avoid repeats
                lastDate = msg["date"]
                break

            if msg["tid"] == msgList[0]["tid"]: # Last message in msgList[::-1] reached
                lastDate = msgList[-1]["date"]

            else:
                exchange = self.__class__.__name__
                amount = msg['amount']
                price = msg['price']
                direction = self.normalizeDirectionField[msg['type']]
                ts = msg['date']

                self.insertData(exchange, amount, price, direction, ts)


lastDate = ""

def start():
    base.createConnection("wss://api.zb.com:9999/websocket", 443, Zb)
