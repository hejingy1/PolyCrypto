import datetime
from ipaddress import v4_int_to_packed
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import mplfinance as mpf
import matplotlib.pyplot as plt
# from dateutil.relativedelta import *
# from simulation import construct_illiquidity

def plot_trending(data_set, name):
    plt.plot(data_set)
    plt.savefig("%s"%name)


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