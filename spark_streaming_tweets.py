from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, udf
from pyspark.sql.types import StructType, StringType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaElasticTweetsApp") \
    .config("spark.jars", "elasticsearch-spark-30_2.12-8.5.0.jar") \
    .config("es.nodes", "localhost") \
    .config("es.port", "9200") \
    .config("es.index.auto.create", "true") \
    .master("local[*]") \
    .getOrCreate()

TWITTER_TOPIC = 'tesla_twitter_data'

# Kafka Configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = TWITTER_TOPIC

# Define the Schema for Incoming Kafka Data
schema = StructType() \
    .add("time", StringType()) \
    .add("username", StringType()) \
    .add("followers", StringType()) \
    .add("tweet", StringType())
    

# Read Stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON from Kafka stream
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Remove "\r" from tweet content
cleaned_stream = parsed_stream.withColumn("tweet", regexp_replace(col("tweet"), r"\r", ""))

# Convert 'followers' to full numbers
def followers_to_number(followers):
    if followers.endswith("M"):
        return int(float(followers[:-1]) * 1_000_000)
    elif followers.endswith("K"):
        return int(float(followers[:-1]) * 1_000)
    else:
        return int(followers)

# Register UDF
followers_to_number_udf = udf(followers_to_number, StringType())
cleaned_stream = cleaned_stream.withColumn("followers", followers_to_number_udf(col("followers")))

# Output the Cleaned Stream to Console
output_path = "/user/root/Tweets"

#query = cleaned_stream.writeStream \
#    .outputMode("append") \
#    .format("json") \
#    .option("path", output_path) \
#    .option("checkpointLocation", "/user/root/Tweets/checkpoint") \
#    .start()

# Write to Elasticsearch
elasticsearch_options = {
    "es.nodes": "localhost",
    "es.port": "9200",
    "es.resource": "twitter_data",
    "es.write.operation": "index",
    "es.spark.sql.streaming.sink.log.enabled": "true"
}

query_elasticsearch = cleaned_stream.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .options(**elasticsearch_options) \
    .option("checkpointLocation", "/user/root/Tweets/elasticsearch_checkpoint") \
    .start()

# Await Termination
#query.awaitTermination()
query_elasticsearch.awaitTermination()
