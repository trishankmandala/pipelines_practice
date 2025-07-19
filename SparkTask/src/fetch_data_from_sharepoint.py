from io import BytesIO
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
import pandas as pd, configparser, json,boto3

def extract_and_load_data(bucket_name,s3_prefix):
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    username    = config['SharePoint']['username']
    password    = config['SharePoint']['password']
    site_url    = config['SharePoint']['site_url']
    folder_path = config['SharePoint']['folder_path']  

    aws_access_key = config['AWS']['aws_access_key']
    aws_secret_access_key = config['AWS']['aws_secret_access_key']
    region_name = config['AWS']['region_name']

    s3 = boto3.client(
        's3',
        aws_access_key_id = aws_access_key,
        aws_secret_access_key = aws_secret_access_key,
        region_name = region_name
    )

    ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))

    folder = ctx.web.get_folder_by_server_relative_url(folder_path)
    files  = folder.files.get().execute_query()

    for file in files:
        if file.name.endswith('.parquet'):
           file_stream = BytesIO()
           ctx.web.get_file_by_server_relative_url(file.serverRelativeUrl).download(file_stream).execute_query()
           file_stream.seek(0)
           

           s3.upload_fileobj(Fileobj=file_stream,Bucket=bucket_name,Key=f"{s3_prefix}/{file.name}")



