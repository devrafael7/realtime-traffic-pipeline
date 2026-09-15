from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType
)

spark = SparkSession.builder \
    .appName("TrafficKafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Estrutura das mensagens enviadas pelo Producer
schema = StructType([
    StructField("road_id", IntegerType()),
    StructField("road_name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("current_speed", DoubleType()),
    StructField("free_flow_speed", DoubleType()),
    StructField("current_travel_time", IntegerType()),
    StructField("confidence", DoubleType()),
    StructField("road_closure", BooleanType())
])


# Lê o Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "traffic-data") \
    .option("startingOffsets", "latest") \
    .load()


# Kafka entrega o value como bytes
messages = df.select(
    col("value").cast("string").alias("json")
)


# Converte o JSON para colunas
traffic = messages.select(
    from_json(col("json"), schema).alias("data")
).select("data.*")


# Mostra os eventos recebidos
query = traffic.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()