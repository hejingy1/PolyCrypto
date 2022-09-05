from calendar import month
import datetime
from ipaddress import v4_int_to_packed
import pandas as pd
import numpy as np
import mplfinance as mpf
from urllib.request import urlopen
import pyarrow as pa
import pyarrow.parquet as pq
from dateutil.relativedelta import *
from matplotlib import pyplot as plt
from Policy import *
from plotFunction import plot_graph, plot_trending
from sklearn.preprocessing import StandardScaler
import statistics
import os

from PolygonFunctionsCrypto import Get_historical_data

"""
https://api.polygon.io/v3/trades/X:BTC-USD?timestamp=2021-06-20&order=asc&limit=50000&sort=timestamp&apiKey=vyEZKZQExqHm6QEiT7LbMCCNrxvxHU0J
"""

"""
触发节奏， 早晚时间
研究一下怎么维持import的整洁性
"""


#Function calculate the illiquidity from the given span date and add it as a column to the numpy
def construct_illiquidity(data, length):
    eth_dump_np = data
    liquidity_index = ((eth_dump_np[:, 3] - eth_dump_np[:, 2])*10000/eth_dump_np[:, 2])/ eth_dump_np[:, -1]
    eth_dump_np = np.c_[eth_dump_np, liquidity_index]

    #eth_dump_liquidity_sort = np.argsort(liquidity_index)
    eth_dump_sort = eth_dump_np[eth_dump_np[:, 8].argsort()]
    eth_dump_sort_length = eth_dump_sort[0:length, :]
    return eth_dump_sort_length, eth_dump_np


def construct_illiquidity_pandas(data):
    # liquidity_index = ((data["close"] - data["open"])*10000/data["open"])/data["n"]
    liquidity_index = ((data["close"] - data["open"])*100/data["open"])/data["n"]
    data_dump = data.assign(illiquidity=liquidity_index)
    return data_dump


def sort_construct_illiquidity_pandas(data, length):
    data_dump_sort = data.sort_values(by=data.columns[-1])
    data_dump_sort_length = data_dump_sort.iloc[:length]
    return data_dump_sort_length


#Function that find the selling price based on the time given
def find_selling(data, index, selling_time):
    price = 0
    try:
        price = data.loc[index+relativedelta(minutes=+selling_time)]["close"]
    except (IndexError, KeyError) as e:
        price = data.loc[index]["close"]

    return price


def construct_month_signal(data_dump, past_data, selling_time):
    counter = 0
    data_dump_signal = data_dump.assign(illiquidity=0.0)
    data_dump_signal = data_dump.assign(signal=0)
    data_dump_signal = data_dump.assign(selling_price=0.0)
    s_price = 0
    margin = []
    volume = []
    month_split_to_day = np.split(data_dump_signal, (data_dump_signal.shape[0]/1440))
    c = 0

    for day in month_split_to_day:
        c += 1
        illiquidity_value = 0
        morning_time_illiquidity = []
        morning_time_volume = []
        night_time_illiquidity = []
        night_time_volume = []
        margin_day = []
        volume_day = []
        for index, row in day.iterrows():
            past_data.past_volume_total.append(row["illiquidity"])
            #std_volume = (row["n"] - np.mean(past_data.past_volume_total))/np.std(past_data.past_volume_total)
            illiquidity_value = (row["illiquidity"]-np.mean(past_data.past_volume_total))/np.std(past_data.past_volume_total)
            past_data.past_volume_total.popleft()
            signal = policy(illiquidity_value, row["n"], row, past_data)

            # print(illiquidity_value)
            # print("past_data:", past_data.illiquidity_average_morning)
            data_dump_signal.at[index, "illiquidity"] = illiquidity_value
            data_dump_signal.at[index, "signal"] = signal
            volume_day.append(row["n"])

            if signal != 0:
                if signal == 3:
                    counter += 1
                    s_price = find_selling(data_dump_signal, index, selling_time)
                    data_dump_signal.at[index, "selling_price"] = s_price
                    margin_day.append((s_price - row["close"])/row["close"])
                if check_time_interval(row):
                    morning_time_illiquidity.append(illiquidity_value)
                    morning_time_volume.append(row["n"])
                else:
                    night_time_illiquidity.append(illiquidity_value)
                    night_time_volume.append(row["n"])
        margin.append(average_cal(margin_day))
        volume.append(average_cal(volume_day))

        # if c == 15:
        #     print(margin_day)
        #     pandas_to_parquet(day, "special_day")
        #     parquet_to_csv("special_day")
        #     print(past_data.volume_average_morning)
        #     print(past_data.volume_average_night)
        #     print(past_data.illiquidity_average_morning)
        #     print(past_data.illiquidity_average_night)
        past_data.add_new_day(morning_time_illiquidity, morning_time_volume, night_time_illiquidity, night_time_volume)
        # print("past:", past_data.volume_average_morning)
        # print("past_night:", past_data.volume_average_night)
    print(counter)
    return data_dump_signal, average_cal(margin), volume, margin


#create a new column that contain the relative illiquidity index of the relavent data set
def calculate_illiquidity(data):
    liquidity_index = ((data[:, 3] - data[:, 2])*100000/data[:, 2])/ data[:, -1]
    data = np.c_[data, liquidity_index]
    return data


#Function convert the numpy to parquet file and store it
def pandas_to_parquet(data_sorted, name):
    # parquet_table = pa.table({'volume': sorted_numpy[:, 0],'vwap': sorted_numpy[:, 1],'open':sorted_numpy[:, 2],
    #                             'close': sorted_numpy[:, 3],'high': sorted_numpy[:, 4],'low': sorted_numpy[:, 5],
    #                             'time': sorted_numpy[:, 6],'n': sorted_numpy[:, 7], "illiquidity": sorted_numpy[:, 8]})
    data_sorted.to_parquet("%s.parquet" %name)


def parquet_to_pandas(name):
    return pd.read_parquet('%s.parquet'%name, engine='pyarrow')


#Function that turn parquet to csv file and store it
def parquet_to_csv(input_name, output_name):
    df = pd.read_parquet("%s.parquet" %input_name)
    df.to_csv("%s.csv" % output_name)
    


#Function that read the parquet file and add a column to it then turn it back to parquet
def add_column_to_parquet(column, name, column_name):
    df = pd.read_parquet("%s.parquet" %name)
    df["%s" %column_name] = column
    df.to_parquet("%s.parquet" %name)

#Function that calculate the average profit based on two given price name and the length of the data
def margin_calculator(name, length, buying_name, selling_name):
    df = pd.read_parquet("%s.parquet" %name)
    a = (df["%s" %selling_name] - df["%s" %buying_name])/df["%s" %buying_name]
    a = a.to_numpy()
    return a.sum()/length

def month_list_generator(length, starting_month):
    month_list = [starting_month + relativedelta(months=+x) for x in range(length)]
    return month_list




"""
The longer the selling time clearly indicates the bigger risk that holding such asset would carry, no clear method to offset it"""
#[0.0008910833452690574, 0.0007820495657642039, 0.0006370691982119203, 0.000922312342715534, 0.0005484461393270147, 0.0006075630654588654, 0.0002326166320267455, 8.34679793868154e-05, 0.0001399186734747146, 0.0001310090039397384, 6.367059632772908e-05]
#[0.0008610277487244006, 0.0021107605852253406, 0.0007997255530908132, 0.0004217144022950859, 0.0003675114988143117, 0.00035448042911426307, 0.00018105105263857435, 8.35535151883573e-06, -3.1633374597974755e-06, 4.259964468678959e-05, 4.801876391391465e-05]
#[0.00035448042911426307, 0.00018105105263857435, 8.35535151883573e-06, -3.1633374597974755e-06, 4.259964468678959e-05, 4.801876391391465e-05, -1.0940526399025674e-05, -8.298291432314898e-05, 5.504807386407333e-06, -1.9252704672376194e-05, 0.00036230234856302]


#iterate through 11 months of data

if __name__ == "__main__":
    """
    collect_times = 600
    selling_time = 2
    a = []
    starting_month = datetime.date(2021, 6, 20)
    month_list = month_list_generator(11, starting_month)
    for i in month_list:
        eth_dump = Get_historical_data("X:ETHUSD", i, i+relativedelta(months=+1), span="minute")
        #filter non-active trading hours
        eth_dump_cut = eth_dump.between_time("03:00", "22:00")
        eth_dump_np = eth_dump.to_numpy()
        eth_dump_np_cut = eth_dump_cut.to_numpy()

        eth_illiq, eth_illiq_useless = construct_illiquidity(eth_dump_np_cut, collect_times)
        numpy_to_parquet(eth_illiq, "eth_dump")
        selling_price = find_selling(eth_dump_np, eth_illiq, selling_time)
        # selling_price2 = find_selling(eth_dump_np, eth_illiq, selling_time+1)
        add_column_to_parquet(selling_price, "eth_dump", "price_after")
        # add_column_to_parquet(selling_price2, "eth_dump", "price_after_2")
        a.append(margin_calculator("eth_dump", collect_times, "close", "price_after"))
        print(a)

        #parquet_to_csv("eth_dump")
    """
    ticker = "ETH"
    collect_times = 600
    morning_pick = 35
    night_pick = 10
    multiplier = 1
    selling_time = 15
    starting_month = datetime.date(2022, 6, 1)
    past_length = 1


    past_two = Pastdata(starting_month, past_length)
    past_two.fetch_past_data(ticker)
    past_two.calculate_day_volume_average(morning_pick, night_pick, multiplier)
    #month_list = month_list_generator(12, starting_month)
    #eth_dump = Get_historical_data(ticker, month_list[2], month_list[2]+relativedelta(months=+1, days=-1), span="minute")
    eth_dump = parquet_to_pandas("parquet_data/ETH/%s-%02d-%02d"%(starting_month.year, starting_month.month+past_length, starting_month.day))
    
    eth_dump = construct_illiquidity_pandas(eth_dump)
    eth_dump_signal, margin_rate, volume, total_margin = construct_month_signal(eth_dump, past_two, selling_time)
    # plot_trending(volume, "2021-01-01")
    print(margin_rate)
    print(total_margin)

    # pandas_to_parquet(eth_dump_signal, "doge_dump_signal")
    # parquet_to_csv("doge_dump_signal")

    #plot the graph of each day
    newpath = "images/%s/%s_%02d"%(ticker, starting_month.year, starting_month.month+1)
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    month_split_to_day = np.split(eth_dump_signal, (eth_dump_signal.shape[0]/1440))
    for i in range(len(month_split_to_day)):
        plot_graph(month_split_to_day[i].iloc[:, 0:8], month_split_to_day[i], newpath+"/%02d-%02d"%(starting_month.month+past_length, i+1))                      
    
