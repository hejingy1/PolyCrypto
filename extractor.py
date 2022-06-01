from sqlite3 import Timestamp
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Market, Feed
from typing import List

import time

price = []
timestamp = []
id = []
average30s = 0
# average1m = 0
# average3m = 0
# average5m = 0

#vyEZKZQExqHm6QEiT7LbMCCNrxvxHU0J


def client_create(pair_type):
    return WebSocketClient(market=Market.Crypto, subscriptions=[pair_type], api_key="vyEZKZQExqHm6QEiT7LbMCCNrxvxHU0J", feed=Feed.RealTime, raw=False)


def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m.size)
        price.append(m.price)
        timestamp.append(m.timestamp)
        id.append(m.id)
        if m.timestamp - timestamp[0] >= 30000:
            average30s = sum(price)/len(price)
            del price[0]
            del timestamp[0]
            del id[0]
            #print(average30s, len(price))



eth = client_create("XT.ETH-USD")

eth.run(handle_msg)