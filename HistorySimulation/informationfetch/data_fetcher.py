import datetime
from HistorySimulation.simulation.PolygonFunctionsCrypto import Get_historical_data
from HistorySimulation.simulation.simulation import *
from dateutil.relativedelta import *
import requests
import pandas as pd
import json
import os



def ticker_search(coin_name):
    url = "https://api.livecoinwatch.com/coins/single"
    headers = {
    'content-type': 'application/json',
    'x-api-key': 'd9981417-0a50-444a-af12-5f2a8d8fafd1'
    }

    payload = json.dumps({
    "currency": "USD",
    "code": "%s"%coin_name,
    "meta": True
    })     

    response = requests.request("POST", url, headers=headers, data=payload)
    a = response.json()
    try:
        return a["cap"], a["cap"]/a["circulatingSupply"]
    except:
        return coin_name

def all_ticker():
    url = f"https://api.polygon.io/v3/reference/tickers?market=crypto&date=2022-08-01&active=true&sort=ticker&order=asc&limit=5000&apiKey=vyEZKZQExqHm6QEiT7LbMCCNrxvxHU0J"
    response = urlopen(url,timeout = 2)
    df = pd.read_json(response.read().decode("utf-8"))
    df_2 = list(df['results'])
    name_list = [x["base_currency_symbol"] for x in df_2]
    return name_list


def ticker_list_generator():
    ticker_information = {}
    error_ticker = []

    ticker = all_ticker()
    for i in ticker:
        a = ticker_search(i)
        if a != i:
            ticker_information[i] = a
        else:
            error_ticker.append(a)

    df = pd.DataFrame.from_dict(ticker_information, orient="index", columns=["cap", "price"])
    pandas_to_parquet(df, "Ticker_pool")
    parquet_to_csv("Ticker_pool", "Ticker_pool")
