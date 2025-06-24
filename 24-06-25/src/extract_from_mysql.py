import configparser
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
def extract_table(table_name:str):
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    username = config['MySqlDB']['username']
    password = config['MySqlDB']['password']
    host     = config['MySqlDB']['host']
    database = config['MySqlDB']['database']
    driver   = config['MySqlDB']['driver']

    encoded_password = urllib.parse.quote_plus(password)

    connection_string = f"mysql+{driver}://{username}:{encoded_password}@{host}/{database}"
    engine = create_engine(connection_string)

    #fetch orders table from MYSQLDB
    fetch_table = pd.read_sql_table(table_name,con=engine)

    return fetch_table


