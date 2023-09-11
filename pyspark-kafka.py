from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CassandraExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042").getOrCreate()

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

print("the data frame is ", df)

# Print the received data to the console
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Cassandra configuration
cassandra_host = "localhost"  # Replace with your Cassandra host
cassandra_port = "9042"       # Replace with your Cassandra port
cassandra_keyspace = "your_keyspace"  # Replace with your Cassandra keyspace
cassandra_table = "your_table"        # Replace with your Cassandra table

# Write data to Cassandra
# query.writeStream \
#     .foreachBatch(lambda batch_df, batch_id: batch_df.write \
#         .format("org.apache.spark.sql.cassandra") \
#         .option("keyspace", cassandra_keyspace) \
#         .option("table", cassandra_table) \
#         .mode("append") \
#         .save()) \

# Start the streaming query
query.awaitTermination()
