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
        self.row_length = 0
        self.past_data = None
        self.volume_average_morning = 0
        self.volume_average_night = 0
        self.price_change_average_morning = 0
        self.price_change_average_night = 0
    
    def fetch_past_data(self, type):
        month_list = month_list_generator(self.month_length, self.starting_date)
        for month in month_list:
            data_dump = Get_historical_data("%s" %type, month, month+relativedelta(months=+1), span="minute")
            if self.past_data == None:
                self.past_data = data_dump
            else:
                self.past_data = pd.concat([self.past_data, data_dump], axis=0)
        self.row_length = self.row_length
    
    #morning pick and night pick represent the amount of times I would trigger the signal between morning and night
    def calculate_day_volume_average(self, morning_pick, night_pick):
        month_split_to_day = np.split(self.past_data, (self.row_length/1440))
        for day in month_split_to_day:
            day_night = day.between_time("22:00", "03:00")
            day_morning = day.between_time("03:00", "22:00")
            day_night_sorted = construct_illiquidity_pandas(day_night, night_pick)
            day_morning_sorted = construct_illiquidity_pandas(day_morning, morning_pick)

            self.volume_average_morning = day_morning_sorted["n"].sum()
            self.volume_average_night = day_night_sorted["n"].sum()
            self.price_change_average_morning = (day_morning_sorted["illiquidity"]*day_morning_sorted["n"]).sum()
            self.price_change_average_night = (day_night_sorted["illiquidity"]*day_night_sorted["n"]).sum()

        self.volume_average_morning = self.volume_average_morning/self.row_length
        self.volume_average_night = self.volume_average_night/self.row_length
        self.price_change_average_morning = self.price_change_average_morning/self.row_length
        self.price_change_average_night = self.price_change_average_night/self.row_length
    
    def new_day(self, new_day, morning_or_not):
        #there might be a problem where the input day is morning but the first day is night thus it would create a small understanding error
        if morning_or_not:
            self.volume_average_morning = self.volume_average_morning-(self.past_data["n"].iloc[0]/self.row_length)+(new_day["n"]/self.row_length)
            self.price_change_average_morning = self.price_change_average_morning-(self.past_data["illiquidity"].iloc[0]*self.past_data["n"].iloc[0]/self.row_length)+(new_day["illiquidity"].iloc[0]*new_day["n"].iloc[0]/self.row_length)
        else:
            self.volume_average_night = self.volume_average_night-(self.past_data["n"].iloc[0]/self.row_length)+(new_day["n"]/self.row_length)
            self.price_change_average_night = self.price_change_average_night-(self.past_data["illiquidity"].iloc[0]*self.past_data["n"].iloc[0]/self.row_length)+(new_day["illiquidity"].iloc[0]*new_day["n"].iloc[0]/self.row_length)
            
        self.past_data = self.past_data.iloc[1:, :]
        self.past_data = pd.concat([self.past_data, new_day], axis=0)






def policy():
    pass



