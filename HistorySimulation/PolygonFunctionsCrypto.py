from urllib.request import urlopen
import pandas as pd
import numpy as np

import datetime
import time
from dateutil import tz

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

ET = tz.gettz('America/New_York')
USERT = tz.gettz()
API_KEY = "vyEZKZQExqHm6QEiT7LbMCCNrxvxHU0J"


def ts_to_easterndatetime(ts): 
    return datetime.datetime.fromtimestamp(ts/1000).replace(tzinfo=USERT).astimezone(ET).replace(tzinfo=None)

def ts_to_easterndatetime_nano(ts): 
    return datetime.datetime.fromtimestamp(ts/1000000000).replace(tzinfo=USERT).astimezone(ET).replace(tzinfo=None)

def datetime_to_ts_nano(date):
    return int(datetime.datetime.timestamp(date.replace(tzinfo=ET).astimezone(USERT).replace(tzinfo=None))*1000000000)

def Get_historical_data(symbol,start_date,end_date,multiplier = 1, span = 'day',adjust = 'true'):
    def Date_to_str(date):
        return date.strftime('%Y-%m-%d')
    for robust in range(3):
        try:
            url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{str(multiplier)}/{span}/{Date_to_str(start_date)}/{Date_to_str(end_date)}?adjusted={adjust}&sort=asc&limit=50000&apiKey={API_KEY}'
            response = urlopen(url,timeout = 2)
            df = pd.read_json(response.read().decode("utf-8"))
            df_2 = pd.DataFrame(list(df['results']))
            df_2.columns = ['volume','vwap','open','close','high','low','time','n']
            df_2['time'] = df_2['time'].apply(ts_to_easterndatetime)
            df_2.index = list(df_2['time'])
            return df_2
        except:
            0
    print('Get_historical_data Error!')
    df_2 = pd.DataFrame(columns = ['volume','vwap','open','close','high','low','time','n'])
    return df_2

def Get_NBBO_quote(symbol,start_date,end_date):
    for robust in range(3):
        try:
            start_ts_nano = datetime_to_ts_nano(start_date)
            end_ts_nano = datetime_to_ts_nano(end_date)
            url = f'https://api.polygon.io/v3/quotes/{symbol}?timestamp.gte={start_ts_nano}&timestamp.lte={end_ts_nano}&order=asc&limit=50000&sort=timestamp&apiKey={API_KEY}'
            response = urlopen(url,timeout = 2)
            df = pd.read_json(response.read().decode("utf-8"))
            df_2 = pd.DataFrame(list(df['results']))
            return df_2
        except:
            0
    print('Get_NBBO_quote Error!')
    df_2 = pd.DataFrame(columns = ['bid_exchange', 'bid_price', 'bid_size', 'conditions',
       'participant_timestamp', 'sequence_number', 'sip_timestamp', 'tape',
       'ask_exchange', 'ask_price', 'ask_size'])
    return df_2

def Get_trades(symbol,start_date,end_date):
    for robust in range(3):
        try:
            start_ts_nano = datetime_to_ts_nano(start_date)
            end_ts_nano = datetime_to_ts_nano(end_date)
            url = f'https://api.polygon.io/v3/trades/{symbol}?timestamp.gte={start_ts_nano}&timestamp.lte={end_ts_nano}&order=asc&limit=50000&sort=timestamp&apiKey={API_KEY}'
            response = urlopen(url,timeout = 2)
            df = pd.read_json(response.read().decode("utf-8"))
            df_2 = pd.DataFrame(list(df['results']))
            return df_2
        except:
            0
    df_2 = pd.DataFrame(columns = ['conditions', 'exchange', 'id', 'participant_timestamp', 'price',
       'sequence_number', 'sip_timestamp', 'size', 'tape'])
    return df_2