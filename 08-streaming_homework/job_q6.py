import os
from pyflink.table import EnvironmentSettings, TableEnvironment

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(settings)
t_env.get_config().set("parallelism.default", "1")
t_env.get_config().set("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")

t_env.execute_sql("""
    CREATE TABLE green_trips (
        lpep_pickup_datetime STRING,
        tip_amount DOUBLE,
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:29092',
        'properties.group.id' = 'q6-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# 1-hour tumbling window for total tip amount
print("\n--- Executing Question 6: Largest Tip per Hour ---")
t_env.execute_sql("""
    SELECT 
        window_start, 
        SUM(tip_amount) as total_tip
    FROM TABLE(
        TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '1' HOURS))
    GROUP BY window_start, window_end
    ORDER BY total_tip DESC
    LIMIT 5
""").print()
