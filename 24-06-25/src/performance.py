import pandas as pd
import configparser
import urllib.parse
from sqlalchemy import create_engine, text
def find_performance():
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
    
    query = '''
        select customer_id,count(order_id),sum(order_amount) as total_purchases
        from orders
        where order_status = 'Completed'
        group by customer_id
        order by total_purchases desc
    '''
    new_table = pd.read_sql_query(query,con=engine)
    return new_table

