import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Question 1: Spark version
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('homework_2026') \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED") \
    .getOrCreate()

print(f"Question 1 (Spark version): {spark.version}")

# Question 2: Read Yellow Nov 2025 Parquet
# We read parquet directly now
df = spark.read.parquet('yellow_tripdata_2025-11.parquet')

# Repartition the Dataframe to 4 partitions and save it back to parquet
df.repartition(4).write.parquet('yellow_2025_11_repartitioned/', mode='overwrite')
print("Question 2: Repartitioned to 4 and saved to 'yellow_2025_11_repartitioned/'")

# Question 3: Count records on November 15th
# Note: Use tpep_pickup_datetime
count_nov15 = df.filter(F.to_date(df.tpep_pickup_datetime) == '2025-11-15').count()
print(f"Question 3 (Trips on Nov 15): {count_nov15}")

# Question 4: Longest trip in hours
# Calculating duration (dropoff - pickup) in hours
df_duration = df.withColumn('duration_hrs', 
    (F.unix_timestamp(df.tpep_dropoff_datetime) - F.unix_timestamp(df.tpep_pickup_datetime)) / 3600
)
max_duration = df_duration.select(F.max('duration_hrs')).collect()[0][0]
print(f"Question 4 (Longest trip in hrs): {max_duration}")

# Question 6: Least frequent pickup location zone
zones_df = spark.read.option("header", "true").csv('taxi_zone_lookup.csv')

# Finding the zone with the minimum number of pickups
least_popular = df.groupBy('PULocationID') \
    .count() \
    .join(zones_df, df.PULocationID == zones_df.LocationID) \
    .orderBy('count', ascending=True) \
    .select('Zone', 'count') \
    .limit(1) \
    .collect()[0]

print(f"Question 6 (Least popular zone): {least_popular['Zone']} with {least_popular['count']} trips")
