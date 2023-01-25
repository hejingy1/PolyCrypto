from simulation import parquet_to_pandas, month_list_generator, parquet_to_csv, pandas_to_parquet
from PolygonFunctionsCrypto import Get_historical_data
from dateutil.relativedelta import *
from scipy import stats
import pandas as pd
import os
import datetime
import numpy as np
import matplotlib.pyplot as plt


def rank_by_cap(name):
    ticker_list = parquet_to_pandas(name)
    a = ticker_list.sort_values(by=["cap"], ascending=False)
    return a.iloc[0:30, :]

def parquet_request(name):
    newpath = 'parquet_data/%s'%name
    ticker = "X:%sUSD"%name

    starting_month = datetime.date(2020, 3, 1)
    month_list = month_list_generator(29, starting_month)

    if not os.path.exists(newpath):
        os.makedirs(newpath)
    for i in month_list:
        data_dump = Get_historical_data(ticker, i, i+relativedelta(months=+1, days=-1), span="minute")
        # print(data_dump.shape[0])

        data_dump.to_parquet("parquet_data/%s/%s.parquet" %(name, i))

def csv_to_pandas(name):
    return pd.read_csv("%s.csv"%name)


starting_month = datetime.date(2022, 1, 1)
month_list = month_list_generator(7, starting_month)
poly_list = Get_historical_data("X:ETHUSD", month_list[0], month_list[6]+relativedelta(months=+1, days=-1), span="day")



testcase1 = csv_to_pandas("testcase12")
testcase2 = csv_to_pandas("testcase2")
poly_list2 = stats.zscore(poly_list["n"])
testcase1_list=stats.zscore(testcase1["volume"])
testcase2_list=stats.zscore(testcase2["Volume"])
b = [np.NaN for x in range(212)]
for i in range(len(poly_list2)):

    if np.abs(poly_list2[i]-testcase1_list[i]) > 2:
        print("Poly:", poly_list2[i])
        print("testcase1:", testcase1_list[i])
        print("testcase2:", testcase2_list[i])
        print(poly_list.iloc[i, -2]) 
        b[i] = poly_list2[i]
    elif np.abs(poly_list2[i]-testcase2_list[i]) > 2:
        print("Poly:", poly_list2[i])
        print("testcase1:", testcase1_list[i])
        print("testcase2:", testcase2_list[i])
        print(poly_list.iloc[i, -2])
        b[i] = poly_list2[i]

a = [x for x in range(212)]
plt.plot(a, testcase1_list, label="testcase1", color="r")
plt.plot(a, testcase2_list, label="testcase2", color="g")
plt.plot(a, poly_list2, label="polylist", color="b")
plt.plot(a, b, "ro")
plt.legend()
plt.show()

# ticker_list = rank_by_cap("HistorySimulation/informationfetch/Ticker_pool")
# for i in ticker_list.index:
#     parquet_request(i)
#     print(i,"is done")