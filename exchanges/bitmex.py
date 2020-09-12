# https://www.bitmex.com/app/wsAPI
import base


class Bitmex(base.Base):

    def onMessage(self, payload, isBinary):
        self.producer.send('bitmexTrades', payload)


class BitmexOB(Bitmex):
    OB = {}
    OBid = {}

    def onMessage(self, payload, isBinary):
        payload = base.json.loads(payload.decode('utf-8'))
        print(payload)
        try:
            if payload['action'] == 'partial':
                self.OB[payload["filter"]["symbol"]] = {"exchange": "bitmex", "pair": payload["filter"]["symbol"], "time": base.time.time_ns() // 1000000, "bids": [], "asks": []}
                for entry in payload['data']:
                    self.OBid["id"] = entry["price"]
                    if entry['side'] == 'Buy':
                        self.OB[payload["filter"]["symbol"]]['bids'].append({'price': float(entry['price']), 'amount': float(entry['size'])})

                    elif entry['side'] == 'Sell':
                        self.OB[payload["filter"]["symbol"]]['asks'].append({'price': float(entry['price']), 'amount': float(entry['size'])})

            elif payload['action'] == 'update':
                for entry in payload['data']:
                    if entry['side'] == 'Sell':
                        for count, ask in enumerate(self.OB[entry['symbol']]['asks']):
                            if ask['price'] == self.OBid[entry['id']]:
                                self.OB[entry['symbol']]['asks'][count]['amount'] = float(entry['size'])
                                self.OB[entry['symbol']]['time'] = base.time.time_ns() // 1000000

                    elif entry['side'] == 'Buy':
                        for count, bid in enumerate(self.OB[entry['symbol']]['bids']):
                            if bid['price'] == self.OBid[entry['id']]:
                                self.OB[entry['symbol']]['bids'][count]['amount'] = float(entry['size'])
                                self.OB[entry['symbol']]['time'] = base.time.time_ns() // 1000000

            elif payload['action'] == 'insert':
                for entry in payload['data']:
                    self.OBid[entry['id']] = entry['price']
                    if entry['side'] == 'Sell':
                        self.OB[entry['symbol']]['asks'].append({'price': float(entry['price']), 'amount': float(entry['size'])})
                        self.OB[entry['symbol']]['time'] = base.time.time_ns() // 1000000

                    elif entry['side'] == 'Buy':
                        self.OB[entry['symbol']]['bids'].append({'price': float(entry['price']), 'amount': float(entry['size'])})
                        self.OB[entry['symbol']]['time'] = base.time.time_ns() // 1000000

            elif payload['action'] == 'delete':
                for entry in payload['data']:
                    if entry['id'] in self.OBid:
                        if entry['side'] == 'Sell':
                            for count, ask in enumerate(self.OB[entry['symbol']]['asks']):
                                if ask['price'] == self.OBid[entry['id']]:
                                    del self.OB[entry['symbol']]['asks'][count]
                                    del self.OBid[entry['id']]
                                    self.OB[entry['symbol']]['time'] = base.time.time_ns() // 1000000

                        elif entry['side'] == 'Buy':
                            for count, bid in enumerate(self.OB[entry['symbol']]['bids']):
                                if bid['price'] == self.OBid[entry['id']]:
                                    del self.OB[entry['symbol']]['bids'][count]
                                    del self.OBid[entry['id']]
                                    self.OB[entry['symbol']]['time'] = base.time.time_ns() // 1000000

            #print(len(self.OB['XBTUSD']['bids']))

        except:
            pass
        #self.producer.send('bitmexOrderBooks', payload)


#base.createConnection("wss://www.bitmex.com/realtime?subscribe={}".format(base.instruments.instruments['bitmex']), 443, Bitmex)
base.createConnection("wss://www.bitmex.com/realtime?subscribe=orderBookL2:XBTUSD", 443, BitmexOB)

base.reactor.run()