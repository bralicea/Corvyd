import base


class Gate(base.Base):

    def sendPingToServer(self):
        self.sendMessage('{"id":12312, "method":"server.ping", "params":[]}'.encode())

    def onOpen(self):
        params = {
            "id":12312,
            "method":"trades.subscribe",
            "params":["BTC_USDT"]
        }
        subscription = base.json.dumps(params)
        self.sendMessage(subscription.encode('utf8'))
        heartbeat = base.task.LoopingCall(self.sendPingToServer)
        heartbeat.start(60)

    def onMessage(self, payload, isBinary):
        self.producer.send('gateTrades', payload)


def start():
    base.createConnection("wss://ws.gate.io/v3/", 443, Gate)


start()
