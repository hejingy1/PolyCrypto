from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Market, Feed
from typing import Union
import json

c = WebSocketClient(market=Market.Crypto, subscriptions=["XT.ETH-USD"], api_key="vyEZKZQExqHm6QEiT7LbMCCNrxvxHU0J", feed=Feed.RealTime, raw=False)

def handle_msg(msgs: Union[str, bytes]):
    print(msgs)


c.run(handle_msg)