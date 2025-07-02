import configparser
import boto3

def fetch_data_from_dynamo(table_name):
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    aws_access_key = config['AWS']['aws_access_key']
    aws_secret_access_key = config['AWS']['aws_secret_access_key']
    region_name = config['AWS']['region_name']

    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    table = dynamodb.Table(table_name)

    response = table.scan()

    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey = response['LastEvaluatedKey'])
        data.extend(response['Items'])
    
    return data

