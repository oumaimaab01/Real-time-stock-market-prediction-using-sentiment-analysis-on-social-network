from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from elasticsearch import Elasticsearch

# Create a Spark session
spark = SparkSession.builder \
    .appName("SparkStreamingElasticsearch") \
    .getOrCreate()

# Elasticsearch client
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])  # Update host and port if needed

# List existing indexes in Elasticsearch
def list_indexes():
    indexes = es.cat.indices(format="json")  # Get all indexes in JSON format
    for index in indexes:
        print(index['index'])

# Show Elasticsearch indexes
list_indexes()

# Stop the Spark session
spark.stop()

