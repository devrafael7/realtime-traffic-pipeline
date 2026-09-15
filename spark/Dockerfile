FROM apache/spark:4.0.1

USER root

WORKDIR /app

COPY traffic_consumer.py /app/traffic_consumer.py

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1", "/app/traffic_consumer.py"]