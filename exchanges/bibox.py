import base


class Bibox(base.Base):

    def onOpen(self):
        for instrument in base.instruments.instruments['bibox']:
            params = {
                "event": "addChannel",
                "channel": "bibox_sub_spot_{}_deals".format(instrument)
            }
            subscription = base.json.dumps(params)
            self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        self.producer.send('biboxTrades', payload)


base.createConnection("wss://push.bibox.com/", 443, Bibox)
