from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, TimestampType, FloatType
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, TimestampType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, udf
from pyspark.sql.types import StructType, StringType, TimestampType

spark = SparkSession.builder \
    .appName("KafkaElasticStcoksApp") \
    .config("spark.jars", "elasticsearch-spark-30_2.12-8.5.0.jar") \
    .config("es.nodes", "localhost") \
    .config("es.port", "9200") \
    .config("es.index.auto.create", "true") \
    .master("local[*]") \
    .getOrCreate()


STOCK_TOPIC = 'tesla_stock_prices'


# 2. Kafka Configuration
kafka_bootstrap_servers = "localhost:9092"  # Adjust as needed
kafka_topic = STOCK_TOPIC

# 3. Define Schema for Incoming Stock Price Data
schema = StructType() \
    .add("time", StringType()) \
    .add("stock_price", FloatType())

# 4. Read Stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()


print('''reading ##########################################''')

# 5. Deserialize Kafka Messages (Assuming JSON Format)
# Parse and cast the data
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")  # Cast stock_price to FloatType


# 7. Real-Time Aggregations
# Calculate the average price in 1-minute time windows

output_path = "/user/root/Stocks"

# Write streaming data to the specified output directory
#query = parsed_stream.writeStream \
#    .outputMode("append") \
#    .format("json") \
#    .option("path", output_path) \
#   .option("checkpointLocation", "/user/root/Stocks/checkpoint")\
#    .start()

elasticsearch_options = {
    "es.nodes": "localhost",  # Replace with your Elasticsearch host
    "es.port": "9200",
    "es.resource": "stocks",  # Replace with your index name in Elasticsearch
    "es.write.operation": "index",  # Use upsert to handle existing documents
    "es.spark.sql.streaming.sink.log.enabled": "true"  # Enable commit log
}

query_elasticsearch = parsed_stream.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .options(**elasticsearch_options) \
    .option("checkpointLocation", "/user/root/Stocks/elasticsearch_checkpoint") \
    .start()


# 9. Await Termination
#query.awaitTermination()
query_elasticsearch.awaitTermination()

