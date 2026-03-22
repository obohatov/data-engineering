import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Initialize Table Environment
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(settings)
t_env.get_config().set("parallelism.default", "1")

# JAR path from Dockerfile
kafka_jar = "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar"
t_env.get_config().set("pipeline.jars", kafka_jar)

# 2. Source DDL (As per homework)
t_env.execute_sql("""
    CREATE TABLE green_trips (
        lpep_pickup_datetime STRING,
        PULocationID INT,
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:29092',
        'properties.group.id' = 'q5-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# 3. Question 5: Session Window (Legacy Syntax for Flink 1.18)
# We group by PULocationID and a SESSION window with a 5-minute gap
print("\n--- Executing Question 5: Session Window ---")
query = """
    SELECT 
        PULocationID, 
        COUNT(*) as trips_in_session
    FROM green_trips
    GROUP BY 
        PULocationID, 
        SESSION(event_timestamp, INTERVAL '5' MINUTES)
    ORDER BY trips_in_session DESC
    LIMIT 5
"""

t_env.execute_sql(query).print()
