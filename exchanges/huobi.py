import base


class Huobi(base.Base):

    def sendPingToServer(self, ping):
        # Check if websocket connection is open
        if self.state == 3:
            msg = str(base.json.dumps({'pong': ping}))
            self.sendMessage(msg.encode('utf-8'))

    def onOpen(self):
        for instrument in base.instruments.instruments['huobi']:
            msgParams = {
              "sub": "market.{}.trade.detail".format(instrument),
              "id": "id1"
            }
            subscription = base.json.dumps(msgParams)
            self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        msg = base.json.loads(base.zlib.decompress(payload, base.zlib.MAX_WBITS | 32))
        if 'ping' in (msg):
            self.sendPingToServer((msg['ping']))
        else:
            self.producer.send('huobiTrades', payload)


base.createConnection("wss://api.huobi.pro/ws", 443, Huobi)
