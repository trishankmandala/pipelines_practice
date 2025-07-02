import configparser
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
import urllib

def load_to_ssms(data):
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    username = config['Sql']['username']
    password = config['Sql']['password']
    server = config['Sql']['server']
    driver = config['Sql']['driver']
    database = config['Sql']['database']

    params = urllib.parse.quote_plus(
        f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )

    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    df = pd.DataFrame(data)

    # to prevent the jumbled order of columns 
    desired_columns = [
    'project_id', 'project_name', 'client', 'domain', 'location',
    'technologies', 'project_manager', 'start_date', 'end_date', 'status'
     ]
    
    df = df[desired_columns]
    for col in df.columns:
        df[col] = df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    df.to_sql('data_from_dynamoDB',index=False,if_exists='replace',con=engine)


