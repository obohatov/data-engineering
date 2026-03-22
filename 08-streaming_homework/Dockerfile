# Use the official Flink image with Java 11 as a base
FROM flink:1.18.0-java11

# Install Python 3.10 and build dependencies
RUN apt-get update -y && \
    apt-get install -y python3.10 python3-pip python3.10-dev && \
    rm -rf /var/lib/apt/lists/*

# Set python3.10 as the default python
RUN ln -s /usr/bin/python3.10 /usr/bin/python

# Install Apache Flink Python API (PyFlink)
RUN pip3 install apache-flink==1.18.0

# Pre-download Kafka connector JAR to avoid "https scheme" errors during runtime
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar
