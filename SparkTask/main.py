from src.read_data import read_parquet_files_separately
from src.load_to_ssms import load_to_ssms

data = read_parquet_files_separately()
load_to_ssms(data)



