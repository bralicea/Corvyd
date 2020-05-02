import base

class Bitmex(base.Base):

    def onMessage(self, payload, isBinary):
        msg = base.json.loads(payload.decode('utf8'))
        try:
            msg = msg['data'][0]
            exchange = self.__class__.__name__
            amount = msg['size']
            price = msg['price']
            direction = self.normalizeDirectionField[msg['side'].lower()]
            ts = msg['timestamp']
            dt = base.datetime.strptime(ts.split('.')[0], "%Y-%m-%dT%H:%M:%S")
            ts = int((dt - base.datetime.utcfromtimestamp(0)).total_seconds())

            self.insertData(exchange, amount, price, direction, ts)

        except:
            pass

def start():
    base.createConnection("wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD", 443, Bitmex)
