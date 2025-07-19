from pyspark.sql import SparkSession
import configparser
import boto3
import os

def get_spark_session():
    config = configparser.ConfigParser()
    config.read(r'D:\PythonTutorial\config.ini')

    spark = SparkSession.builder \
        .appName("ReadMultipleParquets") \
        .config("spark.hadoop.fs.s3a.access.key", config['AWS']['aws_access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['AWS']['aws_secret_access_key']) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint.region", config['AWS']['region_name']) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.jars", "path/to/hadoop-aws-3.3.1.jar,path/to/aws-java-sdk-bundle-1.11.1026.jar") \
        .getOrCreate()
    
    return spark, config

def list_parquet_files(bucket_name, prefix, aws_access_key, aws_secret_access_key, region_name):
    s3 = boto3.client(
        's3',
        aws_access_key_id = aws_access_key,
        aws_secret_access_key = aws_secret_access_key,
        region_name = region_name
    )
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.parquet')]
    return files

def read_parquet_files_separately():
    spark, config = get_spark_session()
    bucket_name = 'sharepoint-parquet'
    prefix = 'parquet_files/'

    aws_access_key = config['AWS']['aws_access_key']
    aws_secret_access_key = config['AWS']['aws_secret_access_key']
    region_name = config['AWS']['region_name']

    files = list_parquet_files(bucket_name, prefix, aws_access_key, aws_secret_access_key, region_name)

    dataframes = {}

    for file_key in files:
        file_name = os.path.basename(file_key).split(".")[0]  # e.g., housing.parquet -> housing
        s3_path = f"s3a://{bucket_name}/{file_key}"
        df = spark.read.parquet(s3_path)
        dataframes[file_name] = df
        print(f"Loaded DataFrame for: {file_name} with {df.count()} rows")

    return dataframes

