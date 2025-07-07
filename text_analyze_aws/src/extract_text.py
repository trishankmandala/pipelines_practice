import fitz
import configparser,boto3,io

def extract_text_from_pdf(bucket_name,key):
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
    obj = s3.get_object(Bucket=bucket_name,Key = key)
    pdf_stream = io.BytesIO(obj['Body'].read())

    doc = fitz.open(stream=pdf_stream,filetype="pdf")
    full_text = ""
    for page in doc:
        full_text+=page.get_text()
    
    return full_text

        
    


