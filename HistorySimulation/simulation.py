from asyncore import close_all
import datetime
from ipaddress import v4_int_to_packed
import statistics
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import mplfinance as mpf
from urllib.request import urlopen
import pyarrow as pa


from PolygonFunctionsCrypto import Get_historical_data

"""
爆仓单量可能和稳定币需求上升挂钩，补仓需要稳定币来加速交易补仓
"""



eth_dump = Get_historical_data("X:ETHUSD", datetime.date(2021, 6, 20), datetime.date(2021, 7, 20), span="minute")


def construct_inliquidity(data, length):
    eth_dump_np = data.to_numpy()

    liquidity_index = ((eth_dump_np[:, 3] - eth_dump_np[:, 2])*100000/eth_dump_np[:, 2])/ eth_dump_np[:, -1]
    eth_dump_np = np.c_[eth_dump_np, liquidity_index]

    #eth_dump_liquidity_sort = np.argsort(liquidity_index)
    eth_dump_sort = eth_dump_np[eth_dump_np[:, 8].argsort()]
    eth_dump_sort_length = eth_dump_sort[0:length, :]
    return eth_dump_sort_length


def numpy_to_parquet(sorted_numpy):
    parquet_table = pa.table({'volume': sorted_numpy[:, 0],'vwap': sorted_numpy[:, 1],'open':sorted_numpy[:, 2],
                                'close': sorted_numpy[:, 3],'high': sorted_numpy[:, 4],'low': sorted_numpy[:, 5],
                                'time': sorted_numpy[:, 6],'n': sorted_numpy[:, 7], "inliquidity": sorted_numpy[:, 8]})
    pa.parquet.write_table(parquet_table, "eth_dump.parquet")
    
    




#df_length = pd.DataFrame({"l": [i for i in range(len(eth_dump.index))]})

# plt.plot(eth_dump_liquidity[:, 1], eth_dump_liquidity[:, 0])
# plt.show()
# open_mav10 = eth_dump["close"].rolling(5).mean().values

# mavdf = pd.DataFrame(dict(OpMav10=open_mav10,liquidity=liquidity_index),index=df_length.index)

# apd = mpf.make_addplot(mavdf, type='line')

# mpf.plot(eth_dump,type='candle',volume=True, addplot=apd, style='yahoo', tight_layout=True)




