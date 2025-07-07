import pandas as pd
import boto3,configparser

def upload_pdf(file_path:str,bucket_name,s3_key):
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini') 

    aws_access_key = config['AWS']['aws_access_key']
    aws_secret_access_key = config['AWS']['aws_secret_access_key']
    region_name = config['AWS']['region_name']

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    with open(file_path,"rb") as f:
        s3.upload_fileobj(f,bucket_name,s3_key)