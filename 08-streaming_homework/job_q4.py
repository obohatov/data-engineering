import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Initialize Table Environment
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(settings)

# Set parallelism to 1 as per homework requirements
t_env.get_config().set("parallelism.default", "1")

# Use the local path for the JAR (downloaded during Docker build)
kafka_jar_path = "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar"
t_env.get_config().set("pipeline.jars", kafka_jar_path)

# 2. Define the Source Table (Reading from Redpanda)
# Note: Using internal port 29092
t_env.execute_sql("""
    CREATE TABLE green_trips (
        lpep_pickup_datetime STRING,
        PULocationID INT,
        -- Convert string timestamp to Flink TIMESTAMP
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        -- Define 5-second watermark tolerance
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:29092',
        'properties.group.id' = 'q4-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# 3. Question 4: 5-minute Tumbling window
print("\n--- Executing Question 4 Query ---")
t_env.execute_sql("""
    SELECT 
        window_start, 
        PULocationID, 
        COUNT(*) as num_trips
    FROM TABLE(
        TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES))
    GROUP BY window_start, window_end, PULocationID
    ORDER BY num_trips DESC
    LIMIT 5
""").print()
