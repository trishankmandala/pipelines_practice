import configparser
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import urllib.parse

def scd3_load_data(data):
    # Step 1: Read DB config
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

    # Step 2: Try to read existing SCD3 table
    try:
        existing = pd.read_sql("SELECT * FROM customers_scd3", con=engine)
    except:
        existing = pd.DataFrame()

    # Step 3: First full load
    if existing.empty:
        df = data.copy()
        df.rename(columns={"city": "current_city"}, inplace=True)
        df["previous_city"] = None
        df["updated_at"] = datetime.now()
        df.to_sql("customers_scd3", con=engine, index=False, if_exists="append")
        print("Full load done")
        return

    # Step 4: Incremental Load
    data = data.copy()
    data.set_index("customer_id", inplace=True)
    existing.set_index("customer_id", inplace=True)

    # Find new records
    new_ids = data.index.difference(existing.index)
    new_df = data.loc[new_ids].copy()
    new_df.rename(columns={"city": "current_city"}, inplace=True)
    new_df["previous_city"] = None
    new_df["updated_at"] = datetime.now()

    # Find updated city values
    common_ids = data.index.intersection(existing.index)
    with engine.begin() as conn:
        for cid in common_ids:
            new_city = data.at[cid, "city"]
            old_city = existing.at[cid, "current_city"]

            if new_city != old_city:
                conn.execute(text("""
                    UPDATE customers_scd3
                    SET previous_city = current_city,
                        current_city = :new_city,
                        updated_at = :now
                    WHERE customer_id = :cid
                """), {
                    "new_city": new_city,
                    "now": datetime.now(),
                    "cid": cid
                })

        # Step 5: Insert new records
        if not new_df.empty:
            new_df.reset_index(inplace=True)
            new_df.to_sql("customers_scd3", con=engine, index=False, if_exists="append")
            print(f"Inserted {len(new_df)} new rows")

    print(" Incremental SCD3 load complete")
