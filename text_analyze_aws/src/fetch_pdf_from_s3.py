import pandas as pd
import boto3,configparser

def download_data():
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

    response = s3.list_objects_v2(Bucket = 'pdf-analyze-bucket',Prefix='resumes/')
    resume_files = []
    contents = response.get('Contents', [])

    for item in contents:
        key = item['Key']  
        if key.endswith('.pdf'):
            resume_files.append(key)  
    return resume_files