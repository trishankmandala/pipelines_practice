import configparser
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
def load_data(data):
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
       
        # data.to_sql(name='customers_from_mysql',con=engine,index=False,if_exists='replace')
       
        
        existing_customer_ids = pd.read_sql("SELECT customer_id FROM customers_from_mysql", con=engine)
        
        new_data=data[~data["customer_id"].isin(existing_customer_ids["customer_id"])]
        new_data.to_sql(name='customers_from_mysql',con=engine,index=False,if_exists='append')
