from io import BytesIO
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
import pandas as pd, configparser, json

def extract_data():
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    username    = config['SharePoint']['username']
    password    = config['SharePoint']['password']
    site_url    = config['SharePoint']['site_url']
    folder_path = config['SharePoint']['folder_path']  # Example: 'Documents/dataset/json_files'

    ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))

    folder = ctx.web.get_folder_by_server_relative_url(folder_path)
    files  = folder.files.get().execute_query()

    dataframes = {}
    for file in files:
        if file.name.endswith('.json'):
            response = file.open_binary(ctx,file.serverRelativeUrl)
            content = json.load(BytesIO(response.content))
            df = pd.json_normalize(content)
            dataframes[file.name] = df
    return dataframes
