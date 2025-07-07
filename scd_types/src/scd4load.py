import configparser
from datetime import datetime
import urllib.parse
from sqlalchemy import create_engine,text
import pandas as pd
import urllib

def scd4_load_data(data):
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    username = config['Sql']['username']
    password = config['Sql']['password']
    server = config['Sql']['server']
    database = config['Sql']['database']
    driver = config['Sql']['driver']

    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    try:
        existing_data = pd.read_sql("select * from customers_scd4",con = engine)
    except:
        existing_data = pd.DataFrame()
    
    if existing_data.empty:
        data.to_sql('customers_scd4',con=engine,index=False,if_exists="append")
        return
    
    data.set_index('customer_id',inplace=True)
    existing_data.set_index('customer_id',inplace=True)

    common_ids = data.index.intersection(existing_data.index)
    new_ids = data.index.difference(existing_data.index) 
    new_df = data.loc[new_ids]

    changed_ids=[]
    for cid in common_ids:
        if not data.loc[cid].equals(existing_data.loc[cid]):
            changed_ids.append(cid)
    
    with engine.begin() as con:
        if changed_ids:
            history_rows = existing_data.loc[changed_ids].copy()
            history_rows.reset_index(inplace=True)
            history_rows.to_sql('customers_scd4_history_table',con=engine,index=False,if_exists='append') 

            for cid in changed_ids:
                row = data.loc[cid]
                update_query = text("""
                            update customers_scd4
                            set name = :name,
                            city = :city,
                            email = :email,updated_at = :updated_at
                    WHERE customer_id = :customer_id
                """)
                con.execute(update_query, {
                    "customer_id": cid,
                    "name": row["name"],
                    "city": row["city"],
                    "email": row["email"],
                    "updated_at": datetime.now()
                })

        
        if not new_df.empty:
            new_df["updated_at"] = datetime.now()
            new_df.reset_index(inplace=True)
            new_df.to_sql("customers_scd4", con=engine, index=False, if_exists="append")
        

