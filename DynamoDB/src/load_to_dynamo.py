import boto3
import configparser

def load_data_to_dynamoDB(table_name,data):
    config = configparser.ConfigParser()
    config.read('D:\PythonTutorial\config.ini') 

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

    for item in data:
        try:
            response = table.put_item(Item = item)
            print(f"Inserted: {item['project_id']},{response}")
        except Exception as e:
            print(f"failed to insert : {e}")

