import configparser
from sqlalchemy import create_engine, text
import pandas as pd
import urllib.parse
from datetime import datetime

def scd2_load_data(data):
    # Load config
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    username = config['Sql']['username']
    password = config['Sql']['password']
    server   = config['Sql']['server']
    database = config['Sql']['database']
    driver   = config['Sql']['driver']

    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    # Fetch current active records from target
    try:
        existing_data = pd.read_sql("SELECT * FROM customers_scd2 WHERE is_active = 1", con=engine)
    except:
        existing_data = pd.DataFrame()

    # First-time load
    if existing_data.empty:
        data = data.copy()
        data['start_date'] = datetime.now()
        data['end_date'] = pd.NaT
        data['is_active'] = 1
        data.to_sql(name='customers_scd2', con=engine, index=False, if_exists='append')
        return

    # Set indexes for easy comparison
    data = data.copy()
    data.set_index('customer_id', inplace=True)
    existing_data.set_index('customer_id', inplace=True)

    # Identify updated records (same customer_id but different data)
    common_ids = data.index.intersection(existing_data.index)
    updated_ids = []
    for cid in common_ids:
        if not data.loc[cid].equals(existing_data.loc[cid]):
            updated_ids.append(cid)

    updated_df = data.loc[updated_ids]
    new_df = data.loc[data.index.difference(existing_data.index)]

    with engine.begin() as conn:
        # Mark old records inactive
        for cid in updated_df.index:
            conn.execute(
                text("""
                    UPDATE customers_scd2
                    SET is_active = 0,
                        end_date = :end_date
                    WHERE customer_id = :customer_id AND is_active = 1
                """),
                {"customer_id": cid, "end_date": datetime.now()}
            )

        # Insert new records (updated + completely new)
        combined_df = pd.concat([updated_df, new_df])
        combined_df['start_date'] = datetime.now()
        combined_df['end_date'] = pd.NaT
        combined_df['is_active'] = 1
        combined_df.reset_index(inplace=True)
        combined_df.to_sql(name='customers_scd2', con=conn, index=False, if_exists='append')
