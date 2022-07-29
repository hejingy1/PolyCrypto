import datetime
from ipaddress import v4_int_to_packed
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import mplfinance as mpf
from dateutil.relativedelta import *
from simulation import *

from PolygonFunctionsCrypto import Get_historical_data

class Pastdata:
    def __init__(self, starting_date, month_length):
        self.starting_date = starting_date
        self.month_length = month_length
        self.past_data = None
        self.volume_average = 0
    
    def fetch_past_data(self, type):
        month_list = month_list_generator(self.month_length, self.starting_date)
        for month in month_list:
            data_dump = Get_historical_data("%s" %type, month, month+relativedelta(months=+1), span="minute")
            if self.past_data == None:
                self.past_data = data_dump
            else:
                self.past_data = pd.concat([self.past_data, data_dump], axis=0)
    
    def calculate_day_volume_average(self, night_pick, morning_pick):
        month_split_to_day = np.split(self.past_data, (np.shape(self.past_data)[0]/1440))
        for day in month_split_to_day:
            day_night = day.between_time("22:00", "03:00")
            day_morning = day.between_time("03:00", "22:00")
            day_night_sorted = construct_illiquidity_pandas(day_night, night_pick)
            day_morning_sorted = construct_illiquidity_pandas(day_morning, morning_pick)

            day_morning





def get_past_months_data(month_length, start_month, data):
    month_list = month_list_generator(month_length, start_month)
    for month in month_list:
        eth_dump = Get_historical_data("X:ETHUSD", month, month+relativedelta(months=+1), span="minute")
        month_split_to_day = np.split(eth_dump, (np.shape(eth_dump)[0]/1440))
        for day in month_split_to_day:
            day_night = day.between_time("22:00", "03:00")
            day_morning = day.between_time("03:00", "22:00")

        eth_dump_np = eth_dump.to_numpy()


def calculate_past_