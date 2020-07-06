import base


class Kucoin(base.Base):

    def onOpen(self):
            params = {
                "id": 1545910660739,
                "type": "subscribe",
                "topic": "/market/ticker:all",
                "response": True
            }
            subscription = base.json.dumps(params)
            self.sendMessage(subscription.encode('utf8'))

    def onMessage(self, payload, isBinary):
        print(payload)
        #self.producer.send('kucoinTrades', payload)


base.createConnection("wss://push1-v2.kucoin.com/endpoint?token=vYNlCtbz4XNJ1QncwWilJnBtmmfe4geLQDUA62kKJsDChc6I4bRDQc73JfIrlFaVYIAE0Gv2--MROnLAgjVsWkcDq_MuG7qV7EktfCEIphiqnlfpQn4Ybg==.IoORVxR2LmKV7_maOR9xOg==&[connectId=1545910660739]&acceptUserMessage=true", 443, Kucoin)
base.reactor.run()