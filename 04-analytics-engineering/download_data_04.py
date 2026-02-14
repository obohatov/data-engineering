import os
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
# Список того, что нужно по ДЗ
DATA_TYPES = {
    "yellow": ["2019", "2020"],
    "green": ["2019", "2020"],
    "fhv": ["2019"]
}

for service, years in DATA_TYPES.items():
    for year in years:
        for month in range(1, 13):
            month_str = f"{month:02d}"
            file_name = f"{service}_tripdata_{year}-{month_str}.parquet"
            url = f"{BASE_URL}{file_name}"
            
            # Создаем папку data, если её нет
            os.makedirs("data", exist_ok=True)
            path = f"data/{file_name}"
            
            if os.path.exists(path):
                print(f"Skipping {file_name}, already exists.")
                continue

            print(f"Downloading {file_name}...")
            response = requests.get(url)
            if response.status_code == 200:
                with open(path, "wb") as f:
                    f.write(response.content)
            else:
                print(f"Failed to download {file_name}")

print("All downloads finished!")
