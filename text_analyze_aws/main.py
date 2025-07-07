
from src.fetch_pdf_from_s3 import download_data
from src.extract_text import extract_text_from_pdf
from src.load_to_ssms import load
from src.parse import parse_details
from src.archive import move_to_archive

bucket_name = 'pdf-analyze-bucket'
resume_keys = download_data()

load
for key in resume_keys:
    text = extract_text_from_pdf(bucket_name,key)
    details = parse_details(text)
    load(details)
    move_to_archive(bucket_name,key)




