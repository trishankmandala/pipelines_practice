from src.extract_data import extract_data
from src.transform import transform
from src.load_to_ssms import load_to_ssms
data = extract_data()
tdata = transform(data)
load_to_ssms(tdata)
