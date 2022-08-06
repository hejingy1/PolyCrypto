import datetime
from ipaddress import v4_int_to_packed
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import mplfinance as mpf
# from dateutil.relativedelta import *
# from simulation import construct_illiquidity

# from PolygonFunctionsCrypto import Get_historical_data

# collect_times = 10
# selling_time = 2
# a = []
# starting_month = datetime.date(2021, 1, 20)
# date_list = [starting_month + relativedelta(months=+x) for x in range(11)]


# eth_dump = Get_historical_data("X:ETHUSD", date_list[0], date_list[0]+relativedelta(days=+5), span="minute")
# eth_dump_cut = eth_dump.between_time("22:00", "03:00")

# eth_dump_np = eth_dump_cut.to_numpy()
# eth_dump_np_2 = eth_dump.to_numpy()

# eth_illiq_sorted, eth_illiq = construct_illiquidity(eth_dump_np, collect_times)
# liquidity_index = ((eth_dump_np_2[:, 3] - eth_dump_np_2[:, 2])*10000/eth_dump_np_2[:, 2])/ eth_dump_np_2[:, -1]
# eth_dump_np_2 = np.c_[eth_dump_np_2, liquidity_index]
# # apd = mpf.make_addplot(eth_illiq[:, 3], type='scatter')
# s = []
# for i in eth_dump_np_2:
#     if i[8] <= eth_illiq_sorted[-1, 8] and (i[6].hour>3 and i[6].hour<22):
#         s.append(i[3])
#     else:
#         s.append(np.nan)
# apd = mpf.make_addplot(s, type='scatter', markersize = 8, color='red')
# mpf.plot(eth_dump, type='line', volume=True, style='yahoo', addplot=apd, tight_layout=True)

def plot_graph(data_dump, data_dump_signal, name):
    level1 = []
    level2 = []
    level3 = []
    for index, row in data_dump_signal.iterrows():
        if row["signal"] == 1:
            level1.append(row["close"])
            level2.append(np.nan)
            level3.append(np.nan)
        # elif row["signal"] == 2:
        #     level1.append(np.nan)
        #     level2.append(row["close"])
        #     level3.append(np.nan)
        elif row["signal"] == 3:
            level1.append(np.nan)
            level2.append(np.nan)
            level3.append(row["close"])
        else:
            level1.append(np.nan)
            level2.append(np.nan)
            level3.append(np.nan)
    
    apd = []
    if all([np.isnan(x) == True for x in level1]) and all([np.isnan(x) == True for x in level3]):
        mpf.plot(data_dump, type='line', volume=True, style='yahoo', tight_layout=True, savefig=dict(fname="%s"%name, dpi=300))
    elif all([np.isnan(x) == True for x in level1]):
        apd = [mpf.make_addplot(level3, type='scatter', markersize = 12, color='blue', marker='v')]
        
    elif all([np.isnan(x) == True for x in level3]):
        apd = [mpf.make_addplot(level1, type='scatter', markersize = 12, color='red', marker='^')]
    else:
        apd = [mpf.make_addplot(level1, type='scatter', markersize = 12, color='red', marker='^'), 
        mpf.make_addplot(level3, type='scatter', markersize = 12, color='blue', marker='v')]
    mpf.plot(data_dump, type='line', volume=True, style='yahoo', tight_layout=True, addplot=apd, savefig=dict(fname="%s"%name, dpi=300))
    


#mpf.make_addplot(level2, type='scatter', markersize = 12, color='green'),  


# apd = [mpf.make_addplot(level1, type='scatter', markersize = 12, color='red', marker='^'), 
# mpf.make_addplot(level3, type='scatter', markersize = 12, color='blue', marker='v')]