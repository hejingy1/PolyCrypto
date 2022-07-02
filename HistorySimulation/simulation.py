import datetime
from ipaddress import v4_int_to_packed
import pandas as pd
import numpy as np
import mplfinance as mpf
from urllib.request import urlopen
import pyarrow as pa
import pyarrow.parquet as pq


from PolygonFunctionsCrypto import Get_historical_data

"""
爆仓单量可能和稳定币需求上升挂钩，补仓需要稳定币来加速交易补仓
https://api.polygon.io/v3/trades/X:BTC-USD?timestamp=2021-06-20&order=asc&limit=50000&sort=timestamp&apiKey=vyEZKZQExqHm6QEiT7LbMCCNrxvxHU0J
"""

#132480


eth_dump = Get_historical_data("X:ETHUSD", datetime.date(2021, 6, 20), datetime.date(2021, 7, 20), span="minute")
eth_dump_np = eth_dump.to_numpy()

def construct_illiquidity(data, length):
    eth_dump_np = data
    liquidity_index = ((eth_dump_np[:, 3] - eth_dump_np[:, 2])*100000/eth_dump_np[:, 2])/ eth_dump_np[:, -1]
    eth_dump_np = np.c_[eth_dump_np, liquidity_index]

    #eth_dump_liquidity_sort = np.argsort(liquidity_index)
    eth_dump_sort = eth_dump_np[eth_dump_np[:, 8].argsort()]
    eth_dump_sort_length = eth_dump_sort[0:length, :]
    return eth_dump_sort_length


def numpy_to_parquet(sorted_numpy, name):
    parquet_table = pa.table({'volume': sorted_numpy[:, 0],'vwap': sorted_numpy[:, 1],'open':sorted_numpy[:, 2],
                                'close': sorted_numpy[:, 3],'high': sorted_numpy[:, 4],'low': sorted_numpy[:, 5],
                                'time': sorted_numpy[:, 6],'n': sorted_numpy[:, 7], "illiquidity": sorted_numpy[:, 8]})
    pq.write_table(parquet_table, "%s.parquet" %name)

def parquet_to_csv(name):
    df = pd.read_parquet("%s.parquet" %name)
    df.to_csv("%s.csv" % name)
    
def find_selling(data, data_inliquidity, time_after):
    open_after_time = []
    for i in data_inliquidity:
        a = np.where(data[:, 6] == i[6])[0][0]
        try:
            open_after_time.append(data[a+time_after][2])
        except IndexError:
            open_after_time.append(data[a][2])
    return open_after_time

eth_illiq = construct_illiquidity(eth_dump_np, 1500)
a = find_selling(eth_dump_np, eth_illiq, 3)
numpy_to_parquet(eth_illiq, "eth_dump")
parquet_to_csv("eth_dump")


 



