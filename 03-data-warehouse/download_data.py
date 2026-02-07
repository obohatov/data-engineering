import os
import requests

# Ссылка на репозиторий с данными
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
months = ["01", "02", "03", "04", "05", "06"]

for month in months:
    file_name = f"yellow_tripdata_2024-{month}.parquet"
    url = f"{base_url}{month}.parquet"
    print(f"Downloading {file_name}...")
    
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_name, "wb") as f:
            f.write(response.content)
        print(f"Successfully downloaded {file_name}")
    else:
        print(f"Failed to download {month}")
