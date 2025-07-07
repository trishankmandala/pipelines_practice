import configparser
from datetime import datetime
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd

def meta_data(table_name :str , new_time : datetime):
        #read the config
       
        config = configparser.ConfigParser()
        config.read(r'D:\PythonTutorial\config.ini')
 
        #load credentials
        username = config['Sql']['username']
        password = config['Sql']['password']
        server     = config['Sql']['server']
        database = config['Sql']['database']
        driver   = config['Sql']['driver']
 
        params = urllib.parse.quote_plus(
            f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
       
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
       
    
        check_qury = text('''
                select count(*) from etl_meta_data where table_name = :table_name
        ''')
        insert_query = text('''
                insert into etl_meta_data (table_name,last_load_time) values (:table_name,:new_time)
        ''')
        update_query = text('''
                update etl_meta_data 
                set last_load_time = :new_time
                where table_name = :table_name
        ''')
        with engine.begin() as conn:
                count = conn.execute(check_qury,{"table_name":table_name}).scalar()
                if count == 0:
                        conn.execute(insert_query,{"table_name": table_name,"new_time": new_time})
                else:
                        conn.execute(update_query,{'table_name': table_name,'new_time': new_time})

