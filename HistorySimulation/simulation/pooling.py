from simulation import parquet_to_pandas, month_list_generator
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
# poly_list = []
# for i in month_list:
#     eth_dump = parquet_to_pandas("parquet_data/ETH/%s-%02d-%02d"%(i.year, i.month, i.day))
#     #print(eth_dump.shape[0]/1440)
#     month_split_to_day = np.split(eth_dump, (eth_dump.shape[0]/1440))
#     for j in month_split_to_day:
#         poly_list.append(j["n"].sum())


# for i in range(len(poly_list["n"])):
#     if poly_list[i] != poly_list.iloc[i, -1]:
#         print(poly_list.iloc[i, -2])
poly_list = Get_historical_data("X:ETHUSD", month_list[0], month_list[6]+relativedelta(months=+1, days=-1), span="day")



testcase1 = csv_to_pandas("testcase1")
for index, row in testcase1.iterrows():
    if row["volume"] > 100000000:
        testcase1.at[index, "volume"] = testcase1.at[index, "volume"]/1000
testcase2 = csv_to_pandas("testcase2")
testcase1 = testcase1.iloc[::-1]
poly_list = stats.zscore(poly_list["n"])
testcase1["volume"]=stats.zscore(testcase1["volume"])
testcase2["Volume"]=stats.zscore(testcase2["Volume"])
a = [x for x in range(212)]
#plt.plot(a, testcase1["volume"], label="testcase1")
plt.plot(a, testcase2["Volume"], label="testcase2")
plt.plot(a, poly_list, label="polylist")
plt.show()

# ticker_list = rank_by_cap("HistorySimulation/informationfetch/Ticker_pool")
# for i in ticker_list.index:
#     parquet_request(i)
#     print(i,"is done")