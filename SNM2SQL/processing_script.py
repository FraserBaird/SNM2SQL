import os
from import_key_setup import connect_to_sql
import pandas as pd
from coscal import coscal as cc
import numpy as np
from import_ssc_nmtbs import execute_import
from import_ssc_nmtbs import get_latest_data


def main():
    # define filenames
    filenames = ['C:/CampbellSci/LoggerNet/CR1000_Weather.dat',
                 'C:/CampbellSci/LoggerNet/CR1000_CountsMod.dat',
                 'C:/CampbellSci/LoggerNet/CR1000_CountsUnmod.dat',
                 'C:/CampbellSci/LoggerNet/CR1000XSeries_CountsMod2.dat']
    mod_dates = {}
    updated = {}
    # loop through filenames and store initial modification times
    for file in filenames:
        # store each modification date in dict
        mod_dates[file] = os.path.getmtime(file)
        # initialise False values in the updated dictionary
        updated[file] = True

    # loop forever
    while True:
        # loop through filenames
        for file in filenames:
            # check for changed modification time
            if mod_dates[file] != os.path.getmtime(file):
                mod_dates[file] = os.path.getmtime(file)
                updated[file] = True

        if all_updated(updated):
            # if the data files are all updated then import corrected data from the last processed timestamp until the
            # the most recent timestamp
            last_processed_time = get_latest_data()
            next_ts = last_processed_time + pd.Timedelta('1T')
            corrected_data = get_corrected_data(next_ts)
            # try and connect to the sql server
            connector = connect_to_sql()
            if not connector:
                # if we can't connect then save the data to the buffer file
                save_to_buffer(corrected_data)
            else:
                upload_data(corrected_data, connector)
                connector.close()
            # reset the update check
            for file in filenames:
                updated[file] = False

    return


def get_corrected_data(last_proc):
    # import the raw data
    raw_data = import_data(last_proc)
    # compute absolute humidity from raw data
    raw_and_ah_data = compute_ah(raw_data)
    # correct the data
    corrected_data = cc.apply_corrections(raw_and_ah_data, 'COSMOS-UK', 2.91, ['CTS_MOD', 'CTS_MOD2', 'CTS_UNMOD'])
    col_order = ['PA', 'RH', 'TA', 'Q', 'CTS_MOD', 'CTS_MOD_CORR', 'CTS_MOD2', 'CTS_MOD2_CORR', 'CTS_UNMOD',
                 'CTS_UNMOD_CORR']
    corrected_data = corrected_data[col_order]
    corrected_data.replace({np.nan: None}, inplace=True)
    return corrected_data


def import_data(last_ts):

    weather = import_weather(last_ts)
    mod1 = import_crs('CTS_MOD', last_ts)
    mod2 = import_crs('CTS_MOD2', last_ts)
    unmod = import_crs('CTS_UNMOD', last_ts)

    raw_df = pd.concat((mod1[['CTS_MOD']], mod2[['CTS_MOD2']],
                        unmod[['CTS_UNMOD']], weather[['PA', 'TA', 'RH']]), axis=1)

    return raw_df


def import_weather(last_ts):
    data = pd.read_csv('C:/Campbellsci/LoggerNet/CR1000_Weather.dat', skiprows=4, header=None, parse_dates=[0],
                       na_values='"NAN"')
    data.columns = ['DATE_TIME', 'REC_NUM', 'TA', 'PA', 'RH']
    data.set_index('DATE_TIME', inplace=True)
    data.loc[:, ['TA', 'PA', 'RH']] = data.loc[:, ['TA', 'PA', 'RH']].astype('float')
    data.drop_duplicates(inplace=True)
    return data.loc[last_ts:, :]


def import_crs(id_string, last_ts):
    if id_string == 'CTS_MOD':
        filename = 'C:/Campbellsci/LoggerNet/CR1000_CountsMod.dat'
    elif id_string == 'CTS_MOD2':
        filename = 'C:/Campbellsci/LoggerNet/CR1000XSeries_CountsMod2.dat'
    elif id_string == 'CTS_UNMOD':
        filename = 'C:/Campbellsci/LoggerNet/CR1000_CountsUnmod.dat'
    else:
        raise ValueError('%s not a valid identifier' % id_string)

    data = pd.read_csv(filename, skiprows=4, header=None, parse_dates=[0], na_values='"NAN"', low_memory=False)
    data.columns = ['DATE_TIME', 'REC_NUM', id_string, 'PERIOD', 'TA', 'RH']
    data[[id_string, 'PERIOD', 'TA', 'RH']] = data[[id_string, 'PERIOD', 'TA', 'RH']].astype('float')
    data.set_index('DATE_TIME', inplace=True)
    data.mask(data['PERIOD'] < 45, inplace=True)
    data.drop_duplicates(inplace=True)
    return data.loc[last_ts:, :]


def compute_ah(the_df):
    df = the_df.copy()
    temp = df['TA'].values
    rh = df['RH'].values

    exponential = np.exp((17.67 * temp) / (temp + 243.5))
    ah = (13.2471 * rh * exponential) / (temp + 273.15)

    df.loc[:, 'Q'] = ah

    return df


def save_to_buffer(corr_dat):
    # save buffer data as csv - will append to existing file or create new file if it doesn't exist
    if os.path.exists('buff.dat'):
        corr_dat.to_csv('buff.dat', mode='a', header=False)
    else:
        corr_dat.to_csv('buff.dat')
    update_last_ts(str(corr_dat.index[-1]))
    return


def all_updated(upt_dict):
    ret_val = True
    for key in upt_dict:
        ret_val &= upt_dict[key]

    return ret_val


def upload_data(corr_dat, sql_con):
    corr_dat.reset_index(inplace=True)
    corr_dat['DATE_TIME'] = corr_dat['DATE_TIME'].astype(str)
    buffer = check_buffer()

    if buffer is not None:
        upload(buffer, sql_con)
        os.remove('buff.dat')

    upload(corr_dat, sql_con)
    try:
        update_last_ts(corr_dat['DATE_TIME'].values[-1])
    except IndexError:
        print('Warning: failed to update timestamp')
    
    return


def upload(corr_dat, connector):
    for item in corr_dat.index:
        execute_import(corr_dat.iloc[item, :], connector)
    return


def update_last_ts(date_str):
    file = open('last_ts.csv', 'w')
    file.write(date_str)
    return


def check_buffer():
    if os.path.exists('buff.dat'):
        ret_val = import_buffer()
    else:
        ret_val = None
    return ret_val


def import_buffer():
    buff = pd.read_csv('buff.dat')
    buff_ret = buff.where(pd.notnull(buff), None)
    return buff_ret


if __name__ == "__main__":
    main()
