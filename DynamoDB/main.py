from src.read_json import read_json
from src.load_to_dynamo import load_data_to_dynamoDB
from src.fetch_data_from_dynamoDB import fetch_data_from_dynamo
from src.load_to_ssms import load_to_ssms
from src.fetch_data_from_mongo import read_data

from src.fetch_unstruct_from_mongo import fetch_from_mongo
from src.transform_unstruct import transform_data
from src.load_unstruct_to_ssms import load_unstructured_to_ssms

# #read data from mongodb
# mongo_data = read_data()

# #loading mongo_data to dynamodb
# load_data_to_dynamoDB('Projects',mongo_data)

# #fetch data from dynamodb
# fetched_data = fetch_data_from_dynamo('Projects')

# #load dynamodb data to sqlserver
# load_to_ssms(fetched_data)


#fetch unstructured data from mongo
data = fetch_from_mongo()

#load mongo data to dynamo
load_data_to_dynamoDB('Unstructured_Data',data)

# fetch data from dynamo
fetched_data = fetch_data_from_dynamo('Unstructured_Data')

# transform dynamo data
df_projects, df_clients, df_technologies, df_team_members, df_milestones = transform_data(fetched_data)

# load to ssms
load_unstructured_to_ssms(df_projects,'projectTable')
load_unstructured_to_ssms(df_clients,'ClientTable')
load_unstructured_to_ssms(df_technologies,'TechnologiesTable')
load_unstructured_to_ssms(df_team_members,'TeamMembersTable')
load_unstructured_to_ssms(df_milestones,'MilestonesTable')






