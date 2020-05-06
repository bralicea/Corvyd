from autobahn.twisted.websocket import WebSocketClientProtocol, WebSocketClientFactory
import sys
from twisted.python import log
from twisted.internet import reactor, ssl
from twisted.internet.protocol import ReconnectingClientFactory
import json
from urllib.parse import urlparse
import zlib
import psycopg2
import ast
import time
from datetime import datetime

class Base(WebSocketClientProtocol):

    # Connect to database
    conn = psycopg2.connect(database="postgres", user='bralicea', password='', host='database-1.c8pnybnwk3oh.us-east-2.rds.amazonaws.com', port='5432') # Password omitted
    conn.autocommit = True
    cursor = conn.cursor()

    # Dictionary meant to normalize data type for the 'direction' field in database
    normalizeDirectionField = {True: '1', '1': '1', 'buy': '1',
                               False: '0', '2': '0', 'sell':'0'}

    def insertData(self, exchange, amount, price, direction, ts):
        sql = '''INSERT into trades (exchange, amount, price, direction, ts) values(%s, %s, %s, %s, %s)''';
        data = (exchange, amount, price, direction, ts)
        self.cursor.execute(sql, data)
        self.conn.commit()

    def onConnect(self, response):
        print("Server connected: {0}".format(response.peer))

    def onOpen(self):
        print("WebSocket connection open.")

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {0} bytes".format(len(payload)))

        else:
            print("Text message received: {0}".format(payload.decode('utf8')))

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))

    def clientConnectionFailed(self, connector, reason):
        print("Client connection failed .. retrying ..")
        self.retry(connector)
        self.resetDelay()

    def clientConnectionLost(self, connector, reason):
        print("Client connection lost .. retrying ..")
        self.retry(connector)
        self.resetDelay()


class Connect(WebSocketClientFactory, ReconnectingClientFactory):

    pass


def createConnection(streamName, port, exchange): # Create a Twisted factory
    log.startLogging(sys.stdout)
    hostName = urlparse(streamName).hostname
    factory = Connect(streamName)
    factory.protocol = exchange
    reactor.connectSSL(hostName, port, factory, ssl.ClientContextFactory())
