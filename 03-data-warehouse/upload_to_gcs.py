import os
from google.cloud import storage

# Укажи путь к своему JSON-ключу (если он в корне, то просто имя файла)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "твой-ключ.json"

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

bucket_name = "имя-твоего-бакета"
months = ["01", "02", "03", "04", "05", "06"]

for month in months:
    file_name = f"yellow_tripdata_2024-{month}.parquet"
    upload_to_gcs(bucket_name, file_name, f"taxi_data/{file_name}")
    