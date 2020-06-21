import base


class Bitforex(base.Base):

    def sendPingToServer(self):
        # Check if websocket connection is open
        if self.state == 3:
            self.sendMessage("ping_p".encode())

    def onOpen(self):
        for instrument in base.instruments.instruments['bitforex']:
            params = [{
                "type": "subHq",
                "event": "trade",
                "param": {
                    "businessType": "coin-{}".format(instrument),
                    "size": 1
                }
            }]
            subscription = base.json.dumps(params)
            self.sendMessage(subscription.encode('utf8'))

        heartbeat = base.task.LoopingCall(self.sendPingToServer)
        heartbeat.start(60)

    def onMessage(self, payload, isBinary):
        if payload != b'pong_p':
            self.producer.send('bitforexTrades', payload)


base.createConnection("wss://www.bitforex.com/mkapi/coinGroup1/ws", 443, Bitforex)
