import os
from datetime import datetime
import pandas as pd
from SNM2SQL.coscal import coscal as cc
from SNM2SQL.import_ssc_nmtbs import get_latest_data
import numpy as np


def get_last_line(filename):
    with open(filename, 'rb') as file:
        # from https://stackoverflow.com/questions/46258499/how-to-read-the-last-line-of-a-file-in-python
        # use seek to move straight to end of binary file
        file.seek(-2, os.SEEK_END)
        # check line for new line - loop moves up a line and breaks if the line returns a new line only
        while file.read(1) != b'\n':
            file.seek(-2, os.SEEK_CUR)
        last_line = file.readline().decode()
        file.close()
    return last_line.split(",")


def get_last_data():
    weather_line = get_last_line('C:/Campbellsci/LoggerNet/CR1000_Weather.dat')
    mod_line = get_last_line('C:/Campbellsci/LoggerNet/CR1000_CountsMod.dat')
    unmod_line = get_last_line('C:/Campbellsci/LoggerNet/CR1000_CountsUnmod.dat')

    last_data = {'DateTime': pd.to_datetime(weather_line[0].strip('"')),
                 'MOD': int(mod_line[2]),
                 'UNMOD': int(unmod_line[2]),
                 'PRESS': float(weather_line[3]),
                 'TEMP': float(weather_line[2]),
                 'RH': float(weather_line[4])}
    if int(mod_line[3]) < 50:
        last_data['MOD'] = np.nan
    if int(unmod_line[3]) < 50:
        last_data['UNMOD'] = np.nan
    return last_data


def get_corrected_data(dat_dict):
    corr_dict = dat_dict.copy()
    corr_dict['AH'] = cc.compute_ah(dat_dict['TEMP'], dat_dict['RH'])
    corr_dict['MOD_CORR'] = corr_dict['MOD'] * cc.humidity_correction(corr_dict['AH']) \
                              * cc.pressure_correction(corr_dict['PRESS'], 2.774)
    corr_dict['UNMOD_CORR'] = corr_dict['UNMOD'] * cc.humidity_correction(corr_dict['AH']) \
                              * cc.pressure_correction(corr_dict['PRESS'], 2.774)
    return corr_dict


def process():
    data = get_last_data()
    corr_data = get_corrected_data(data)
    return corr_data


def main():
    previous_minute = datetime.now().minute
    while True:
        current_time = datetime.now()
        if current_time.minute != previous_minute:
            previous_minute = current_time.minute
            while True:
                current_time = datetime.now()
                if current_time.second == 5:
                    data = process()
                    get_latest_data(data)
                    break

    return


if __name__ == "__main__":
    main()
