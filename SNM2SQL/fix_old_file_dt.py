import pandas as pd
import numpy as np
import csv

data = pd.read_csv('C:/Users/fb00395/OneDrive - University of Surrey/Documents/data/SENDAuto/CR1000_Weather.dat.1.backup',
                   skiprows=4, header=None)
data.columns = ['DATE_TIME', 'REC_NUM', 'TEMP', 'PRESS', 'RH', 'N0', 'N1', 'N2']
# funct = lambda string: '%s' % string
# data.loc[:, 'DATE_TIME'] = data.loc[:, 'DATE_TIME'].astype('str')
data[['REC_NUM', 'TEMP', 'PRESS', 'RH']] = data[['REC_NUM', 'TEMP', 'PRESS', 'RH']].astype('float')
data.loc[:, ['DATE_TIME', 'REC_NUM', 'TEMP', 'PRESS', 'RH']].to_csv(
    "C:/Users/fb00395/OneDrive - University of Surrey/Documents/data/SENDAuto/CR1000_CountsWeatherFIXED.dat.backup",
    index=False, date_format='%Y-%m-%d %H:%M:%S', quoting=csv.QUOTE_NONNUMERIC)