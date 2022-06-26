from asyncore import close_all
import datetime
from ipaddress import v4_int_to_packed
import statistics
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import mplfinance as mpf
from urllib.request import urlopen


from PolygonFunctionsCrypto import Get_historical_data

"""
爆仓单量可能和稳定币需求上升挂钩，补仓需要稳定币来加速交易补仓
"""



eth_dump = Get_historical_data("X:ETHUSD", datetime.date(2021, 11, 20), datetime.date(2021, 11, 26), span="hour")


eth_dump_np = eth_dump.to_numpy()

liquidity_index = ((eth_dump_np[:, 3] - eth_dump_np[:, 2])*100000/eth_dump_np[:, 2])/ eth_dump_np[:, -1]

eth_dump_liquidity = np.concatenate([[liquidity_index], [eth_dump_np[:, 6]]], axis=0)
eth_dump_liquidity = eth_dump_liquidity.T
df_length = pd.DataFrame({"l": [i for i in range(len(eth_dump.index))]})

# plt.plot(eth_dump_liquidity[:, 1], eth_dump_liquidity[:, 0])
# plt.show()
open_mav10 = eth_dump["close"].rolling(5).mean().values

mavdf = pd.DataFrame(dict(OpMav10=open_mav10,liquidity=liquidity_index),index=df_length.index)

apd = mpf.make_addplot(mavdf, type='line')

mpf.plot(eth_dump,type='candle',volume=True, addplot=apd, style='yahoo', tight_layout=True)




