from PolygonFunctionsCrypto import Get_historical_data
from simulation import month_list_generator
import datetime
from dateutil.relativedelta import *

ticker = "X:ETHUSD"
starting_month = datetime.date(2022, 1, 1)
month_list = month_list_generator(7, starting_month)
month_length = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
b = []
c = []
print(month_list)
for i in range(len(month_list)):
    eth_dump = Get_historical_data(ticker, month_list[i], month_list[i]+relativedelta(months=+1, days=-1), span="minute")
    if eth_dump.shape[0]/1440 == month_length[i]:
        b.append(True)
    else:
        b.append(False)
    c.append(eth_dump.shape[0]/1440)
    print(eth_dump)
print(b)
print(c)