import configparser
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd

def scd1_load_data(data):
    # 1. Read config
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

    # 2. Try loading existing data
    try:
        existing_data = pd.read_sql("SELECT * FROM customers_scd1", con=engine)
    except:
        existing_data = pd.DataFrame()

    # 3. If table doesn't exist, do initial load
    if existing_data.empty:
        data.to_sql(name='customers_scd1', con=engine, index=False, if_exists='append')
        return

    # 4. Set customer_id as index for easy comparison
    existing_data.set_index('customer_id', inplace=True)
    data.set_index('customer_id', inplace=True)

    # 5. Find common IDs to check for updates
    common_ids = data.index.intersection(existing_data.index)

    updated_rows = []
    for cid in common_ids:
        # Compare only the values (excluding customer_id)
        if not data.loc[cid].equals(existing_data.loc[cid]):
            updated_rows.append(cid)

    updated_df = data.loc[updated_rows]

    # 6. Find new IDs to insert
    new_ids = data.index.difference(existing_data.index)
    new_df = data.loc[new_ids]

    # 7. Update + Insert logic
    with engine.begin() as conn:
        # -- Update changed rows
        for cid, row in updated_df.iterrows():
            update_fields = row.to_dict()
            update_fields["customer_id"] = cid

            set_clause = ", ".join([f"{col} = :{col}" for col in row.index])
            sql = text(f"""
                UPDATE customers_scd1
                SET {set_clause}
                WHERE customer_id = :customer_id
            """)

            conn.execute(sql, update_fields)
