import datetime
from ipaddress import v4_int_to_packed
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import mplfinance as mpf
from dateutil.relativedelta import *
from simulation import *
from collections import deque

from PolygonFunctionsCrypto import Get_historical_data


def check_time_interval(new_row):
    if new_row["time"].hour > 3 and new_row["time"].hour < 22:
        return True
    else:
        return False

def average_cal(list):
    if len(list) != 0:
        return sum(list)/len(list)
    else:
        return 0


def policy(new_row, past_data):
    # under policy added another line that triggers add_new_day to put more relevent data uder the list and in add_new_day we need to re calculate the mean of this list
    if check_time_interval(new_row):
        if new_row["illiquidity"] <= past_data.illiquidity_average_morning:
            if new_row["n"] <= past_data.volume_average_morning:
                return 3
            return 1
            
        elif new_row["n"] <= past_data.volume_average_morning:
            return 0
        return 0
    else:
        if new_row["illiquidity"] <= past_data.illiquidity_average_night:
            if new_row["n"] <= past_data.volume_average_night:
                return 3
            return 1

        elif new_row["n"] <= past_data.volume_average_night:
            return 0
        return 0

            
class Pastdata:
    def __init__(self, starting_date, month_length):
        self.starting_date = starting_date
        self.month_length = month_length
        self.row_length = 0
        self.past_data = pd.DataFrame()

        self.past_illiquidity_morning = deque([])
        self.past_volume_morning = deque([])
        self.past_illiquidity_night = deque([])
        self.past_volume_night = deque([])

        self.volume_average_morning = 0
        self.volume_average_night = 0
        self.illiquidity_average_morning = 0
        self.illiquidity_average_night = 0
    
    def fetch_past_data(self, type):
        month_list = month_list_generator(self.month_length, self.starting_date)
        for month in month_list:
            data_dump = Get_historical_data("%s" %type, month, month+relativedelta(months=+1), span="minute")
            if self.past_data.empty:
                self.past_data = data_dump
            else:
                self.past_data = pd.concat([self.past_data, data_dump], axis=0)
        self.row_length = self.past_data.shape[0]
    
    #morning pick and night pick represent the amount of times I would trigger the signal between morning and night
    def calculate_day_volume_average(self, morning_pick, night_pick, multiplier):
        self.past_data = construct_illiquidity_pandas(self.past_data)
        month_split_to_day = np.split(self.past_data, (self.row_length/1440))
        for day in month_split_to_day:
            day_night = day.between_time("22:00", "03:00")
            day_morning = day.between_time("03:00", "22:00")

            #can be reduced to one only
            day_morning_sorted = sort_construct_illiquidity_pandas(day_morning, morning_pick*multiplier)
            day_night_sorted= sort_construct_illiquidity_pandas(day_night, night_pick*multiplier)
            
            self.past_volume_morning.append(day_morning_sorted["n"].mean())
            self.past_illiquidity_morning.append(day_morning_sorted["illiquidity"].mean())
            self.past_volume_night.append(day_night_sorted["n"].mean())
            self.past_illiquidity_night.append(day_night_sorted["illiquidity"].mean())

            # self.volume_average_morning = day_morning_sorted["n"].mean()
            # self.volume_average_night = day_night_sorted["n"].mean()
            # self.illiquidity_average_morning = day_morning_sorted["illiquidity"].mean()
            # self.illiquidity_average_night = day_night_sorted["illiquidity"].mean()

        # self.morning_pick = morning_pick*multiplier*(self.row_length/1440)
        # self.night_pick = night_pick*multiplier*(self.row_length/1440)

        # self.volume_average_morning = self.volume_average_morning/(self.row_length/1440)
        # self.volume_average_night = self.volume_average_night/(self.row_length/1440)

        # self.illiquidity_average_morning = self.illiquidity_average_morning/(self.row_length/1440)
        # self.illiquidity_average_night = self.illiquidity_average_night/(self.row_length/1440)

        self.volume_average_morning = average_cal(self.past_volume_morning)
        self.illiquidity_average_morning = average_cal(self.past_illiquidity_morning)
        self.volume_average_night = average_cal(self.past_volume_night)
        self.illiquidity_average_night = average_cal(self.past_illiquidity_night)
    
    def add_new_day(self, morning_time_illiquidity, morning_time_volume, night_time_illiquidity, night_time_volume):
        #new day have some problem where each new day is added but only the first percentile should be added
        self.past_volume_morning.popleft()
        self.past_illiquidity_morning.popleft()
        self.past_volume_night.popleft()
        self.past_illiquidity_night.popleft()
        self.past_volume_morning.append(average_cal(morning_time_volume))
        self.past_illiquidity_morning.append(average_cal(morning_time_illiquidity))
        self.past_volume_night.append(average_cal(night_time_volume))
        self.past_illiquidity_night.append(average_cal(night_time_illiquidity))
        self.volume_average_morning = average_cal(self.past_volume_morning)
        self.illiquidity_average_morning = average_cal(self.past_illiquidity_morning)
        self.volume_average_night = average_cal(self.past_volume_night)
        self.illiquidity_average_night = average_cal(self.past_illiquidity_night)



        # if morning_or_not:  
        #     self.volume_average_morning = self.volume_average_morning-(self.past_data["n"].iloc[0]/self.morning_pick)+(new_day["n"]/self.morning_pick)
        #     self.illiquidity_average_morning = self.illiquidity_average_morning-(self.past_data["illiquidity"].iloc[0]/self.morning_pick)+(new_day["illiquidity"]/self.morning_pick)
        # else:
        #     self.volume_average_night = self.volume_average_night-(self.past_data["n"].iloc[0]/self.night_pick)+(new_day["n"]/self.night_pick)
        #     self.illiquidity_average_night = self.illiquidity_average_night-(self.past_data["illiquidity"].iloc[0]/self.night_pick)+(new_day["illiquidity"]/self.night_pick)
        # self.past_data = self.past_data.iloc[1:, :]
        # self.past_data = pd.concat([self.past_data, new_day], axis=0)



    



