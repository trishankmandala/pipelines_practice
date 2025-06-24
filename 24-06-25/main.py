import src.extract_from_mysql as efm
import src.transform as tfm
from src.load_to_ssms import load_to_ssms

import src.performance as p

df = efm.extract_table('orders')
tdata= tfm.transform_table(df)
load_to_ssms(tdata)

print(p.find_performance())
