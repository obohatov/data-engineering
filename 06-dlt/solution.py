import dlt
from dlt.sources.helpers import requests
import json
import duckdb

def ny_taxi_data():
    """
    Generator function to stream Yellow Taxi data for June 2009.
    The data is in JSONL format.
    """
    url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"
    response = requests.get(url, stream=True)
    response.raise_for_status()
    for line in response.iter_lines():
        if line:
            # Parse each line using standard json library
            yield json.loads(line)

# 1. Initialize the dlt pipeline with DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

# 2. Run the pipeline to load data into the 'rides' table
# write_disposition="replace" ensures the table is recreated for a clean run
load_info = pipeline.run(ny_taxi_data(), table_name="rides", write_disposition="replace")
print(f"Pipeline run info: {load_info}")

# 3. Answer Homework Questions using SQL in DuckDB
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

print("\n--- Question 1: Start and End Date ---")
# Querying min and max pickup times using the 2009 column names
dates_query = "SELECT min(trip_pickup_date_time), max(trip_pickup_date_time) FROM rides"
conn.sql(dates_query).show()

print("\n--- Question 2: Credit Card Proportion ---")
# Based on earlier errors, payment_type contains strings like 'Credit' or 'Cash'
cc_query = """
    SELECT 
        (count(CASE WHEN payment_type = 'Credit' THEN 1 END) * 100.0 / count(*)) as credit_card_percentage
    FROM rides
"""
conn.sql(cc_query).show()

print("\n--- Question 3: Total Tips Amount ---")
# Summing up tips using the 2009 column name 'tip_amt'
tips_query = "SELECT sum(tip_amt) FROM rides"
conn.sql(tips_query).show()
