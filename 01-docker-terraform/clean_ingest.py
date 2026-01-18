import pandas as pd
from sqlalchemy import create_engine

# Подключение (используем localhost, так как запускаем из терминала)
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

print("Загружаем зоны...")
df_zones = pd.read_csv('taxi_zone_lookup.csv')
df_zones.to_sql(name='zones', con=engine, if_exists='replace', index=False)

print("Загружаем данные такси за НОЯБРЬ 2025...")
# Используем fastparquet, чтобы избежать ошибки ArrowKeyError
df = pd.read_parquet('green_tripdata_2025-11.parquet', engine='fastparquet')

# Загружаем всё в базу
df.to_sql(name='green_taxi_data', con=engine, if_exists='replace', index=False)

print("ГОТОВО! База чиста и наполнена Ноябрем 2025.")
