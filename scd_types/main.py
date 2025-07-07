from src.extract import extract_table
from src.transform import transform_table
from src.metaData import meta_data
from datetime import datetime


# from scd_types.src.SCD0load import load_data
# from src.scd1load import scd1_load_data
# from src.scd2load import scd2_load_data
# from src.scd3load import scd3_load_data
from src.scd4load import scd4_load_data

# full load 

incoming_data =extract_table('customers','updated_at','2025-07-01 22:56:23.470')
transform_data = transform_table(incoming_data)
# load_data(transform_data)

scd4_load_data(transform_data)
meta_data('customers_scd4',datetime.now())