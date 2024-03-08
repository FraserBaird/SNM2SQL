import pandas as pd

from import_key_setup import connect_to_sql
from import_ssc_nmtbs import execute_import
import pandas as pd
import numpy as np
from datetime import datetime as dt
df = pd.DataFrame({'DATE_TIME': '1970-01-01 04:00:00', 'PA': np.nan, 'RH': 50.6, 'TA': 22.6, 'AH': 10.3,
                   'CTS_MOD': 30, 'CTS_MOD_CORR': 31.3, 'CTS_MOD2': 28, 'CTS_MOD2_CORR': 28.4, 'CTS_UNMOD': 22,
                   'CTS_UNMOD_CORR': 23.4}, index=[0])


df = df.replace({np.nan: None})

connector = connect_to_sql()

execute_import(df, connector)

connector.close()
