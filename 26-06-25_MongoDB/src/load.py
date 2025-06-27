import configparser
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd

def load_to_ssms(data):
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    username = config['SqlDB']['username']
    password = config['SqlDB']['password']
    server   = config['SqlDB']['server']
    database = config['SqlDB']['database']
    driver   = config['SqlDB']['driver']

    params = urllib.parse.quote_plus(
        f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )

    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

    data.to_sql('data_from_MongoDB',con=engine,if_exists = 'replace',index = False)

    