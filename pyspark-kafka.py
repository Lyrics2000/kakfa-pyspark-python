from pyspark.sql import SparkSession

# Create a SparkSession with the Kafka package
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

# Define the Kafka broker address and topic name
bootstrap_servers = 'localhost:9092'  # Change this to your Kafka broker address
topic_name = 'example_topic'  # Change this to your desired Kafka topic

# Define the Kafka source options
kafka_options = {
    "kafka.bootstrap.servers": bootstrap_servers,
    "subscribe": topic_name
}

# Read data from Kafka as a stream
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Print the received data to the console
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Start the streaming query
query.awaitTermination()
