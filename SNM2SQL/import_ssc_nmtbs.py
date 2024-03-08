from datetime import datetime
import pandas as pd


def execute_import(dataframe, sql_conn):
    # convert the dataframe columns to the right types for the sql table
    values = get_values(dataframe)
    # date = datetime.combine(datetime.date(pd.Timestamp(dataframe['DATE_TIME'].values[0])),
    #                         datetime.time(pd.Timestamp(dataframe['DATE_TIME'].values[0])))
    # get sql cursor
    cursor = sql_conn.cursor(buffered=True)
    insert_aws = """INSERT INTO SSC_NMS.ssc_nts (DATETIME,STATION,PA,RH,TA,AH,CTS_MOD,CTS_MOD_CORR,CTS_MOD2,
    CTS_MOD2_CORR,CTS_UNMOD,CTS_UNMOD_CORR) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
    cursor.execute(insert_aws, values)
    sql_conn.commit()

    return


def get_values(df):
    date = datetime.combine(datetime.date(pd.Timestamp(df['DATE_TIME'])),
                            datetime.time(pd.Timestamp(df['DATE_TIME'])))
    values = [date, 'SSC']
    for key in df.index:
        if key == 'DATE_TIME':
            continue
        values.append(get_sql_friendly_value(df[key]))

    return tuple(values)


def get_sql_friendly_value(variable):
    try:
        value = variable.item()
    except AttributeError:
        value = variable

    return value


def get_latest_data():

    file = open('last_ts.csv')
    ret_val = pd.to_datetime(file.readline())

    return ret_val


if __name__ == "__main__":
    get_latest_data()

