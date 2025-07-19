import configparser,pandas as pd,urllib
import urllib.parse
from sqlalchemy import create_engine,text

def load_to_ssms(data:dict):
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    driver = config['Sql']['driver']
    server = config['Sql']['server']
    username = config['Sql']['username']
    password = config['Sql']['password']
    database = config['Sql']['database']

    params = urllib.parse.quote_plus(
        f'''
            DRIVER={{{driver}}};
            SERVER={server};
            DATABASE={database};
            UID={username};
            PWD={password}'''
            )
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    
    with engine.begin() as conn:
        for name,df in data.items():
            df = df.toPandas()
            df.to_sql(name,conn,if_exists='replace',index=False,chunksize=1000)

        

    

