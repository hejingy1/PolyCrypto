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


eth_dump = Get_historical_data("X:ETHUSD", datetime.date(2021, 6, 20), datetime.date(2021, 8, 20), span="minute")
print(eth_dump.next_url)


def construct_inliquidity(data, length):
    eth_dump_np = data.to_numpy()

    liquidity_index = ((eth_dump_np[:, 3] - eth_dump_np[:, 2])*100000/eth_dump_np[:, 2])/ eth_dump_np[:, -1]
    eth_dump_np = np.c_[eth_dump_np, liquidity_index]

    #eth_dump_liquidity_sort = np.argsort(liquidity_index)
    eth_dump_sort = eth_dump_np[eth_dump_np[:, 8].argsort()]
    eth_dump_sort_length = eth_dump_sort[0:length, :]
    return eth_dump_sort_length


def numpy_to_parquet(sorted_numpy, name):
    parquet_table = pa.table({'volume': sorted_numpy[:, 0],'vwap': sorted_numpy[:, 1],'open':sorted_numpy[:, 2],
                                'close': sorted_numpy[:, 3],'high': sorted_numpy[:, 4],'low': sorted_numpy[:, 5],
                                'time': sorted_numpy[:, 6],'n': sorted_numpy[:, 7], "inliquidity": sorted_numpy[:, 8]})
    pq.write_table(parquet_table, "%s.parquet" %name)

def parquet_to_csv(name):
    df = pd.read_parquet("%s.parquet" %name)
    df.to_csv("%s.csv" % name)
    
# eth_inliq = construct_inliquidity(eth_dump, 2000)
# numpy_to_parquet(eth_inliq, "eth_dump")
# parquet_to_csv("eth_dump")


 



