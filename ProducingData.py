from kafka import KafkaProducer
import pandas as pd
import json

def row_to_json(row):
    # Convert row to a dictionary and ensure Timestamps are converted to strings
    row_dict = row.to_dict()
    for key, value in row_dict.items():
        if hasattr(value, 'isoformat'):  # Check if the value is a Timestamp or datetime object
            row_dict[key] = value.isoformat()  # Convert to ISO format string
    return json.dumps(row_dict).encode('utf-8')

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Replace with your Kafka broker's IP and port
TWITTER_TOPIC = 'tesla_twitter_data'
STOCK_TOPIC = 'tesla_stock_prices'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: x  # Send raw JSON
)

# Read Excel file and CSV file
twitter_df = pd.read_excel("Tesla_Reddit_Posts.xlsx")
stock_df = pd.read_csv("stock_data.csv")

# Filter and rename columns for Reddit posts
twitter_df = twitter_df[["Date", "Subreddit", "Followers", "Title"]].rename(columns={
    "Date": "time",
    "Title": "tweet",
    "Subreddit": "username",
    "Followers": "followers"
})

# Filter and rename columns for stock data
stock_df = stock_df[["Date", "Close"]].rename(columns={
    "Date": "time",
    "Close": "stock_price"
})

# Produce Twitter Data
print("Sending Twitter Data to Kafka...")
for _, row in twitter_df.iterrows():
    message = row_to_json(row)
    producer.send(TWITTER_TOPIC, value=message)
    print(f"Sent to {TWITTER_TOPIC}: {message}")

# Produce Stock Prices Data
print("Sending Stock Prices to Kafka...")
for _, row in stock_df.iterrows():
    message = row_to_json(row)
    producer.send(STOCK_TOPIC, value=message)
    print(f"Sent to {STOCK_TOPIC}: {message}")

# Close the producer
producer.close()
print("All data sent successfully to Kafka!")
