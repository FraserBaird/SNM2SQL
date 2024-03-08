import json, requests, os
import urllib.request
import mysql.connector


# ----------------------------------------------
def get_swagger_key():
    this_dir = os.path.dirname(os.path.realpath(__file__))
    # f = open(os.path.join(os.environ['LAMBDA_TASK_ROOT'],"SWAGGER_Key.txt"), "r")
    f = open(os.path.join(this_dir, "SWAGGER_Key.txt"), "r")
    swagger_key = f.read()
    return swagger_key


# ----------------------------------------------
def get_my_sql_details():
    this_dir = os.path.dirname(os.path.realpath(__file__))
    # f = open(os.path.join(os.environ['LAMBDA_TASK_ROOT'],"SQL_Details.txt"), "r")
    f = open(os.path.join(this_dir, "SQL_Details.txt"), "r")
    data = f.read().split('\n')
    return data


# ----------------------------------------------
def build_sql_connector(data):
    user = data[0]
    password = data[1]
    host = data[2]
    database = data[3]

    try:
        cnx = mysql.connector.connect(user=user,
                                      password=password,
                                      host=host,
                                      database=database)
    except mysql.connector.errors.InterfaceError:
        cnx = False
        print('Warning: Could not connect to SQL, will continue to try ad infinitum...')
    return cnx


# ----------------------------------------------
def close_sql_connector(cnx):

    cnx.close()


# ----------------------------------------------
def setup_swagger_headers(swagger_key):
    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer ' + swagger_key}
    return headers


# ----------------------------------------------
def get_data(url, headers):
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    return data


# ----------------------------------------------

def connect_to_sql():
    sql_keys = get_my_sql_details()
    cnctr = build_sql_connector(sql_keys)
    return cnctr


if __name__ == "__main__":
  
    connector = connect_to_sql()
    if connector:
        print('Connected')
    else:
        print('Disconnected')
