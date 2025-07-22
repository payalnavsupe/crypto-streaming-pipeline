import json
import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import snowflake.connector

# Snowflake connection details
SNOWFLAKE_ACCOUNT = "xeuzfvm-xl48714"
SNOWFLAKE_USER = "payalsnavsupe"
SNOWFLAKE_PASSWORD = "Payalsnowflake@1"
SNOWFLAKE_DATABASE = "CRYPTO_STREAMING"
SNOWFLAKE_SCHEMA = "CRYPTO_SCHEMA"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Retry connecting to Kafka
for attempt in range(5):
    try:
        consumer = KafkaConsumer(
            'crypto-prices',
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='crypto-consumer-group'
        )
        print("Connected to Kafka")
        break
    except NoBrokersAvailable:
        print(f"Attempt {attempt + 1}/5: Kafka not ready. Retrying in 5 seconds...")
        time.sleep(5)
else:
    raise Exception("Kafka broker not available after multiple retries.")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)
cursor = conn.cursor()

try:
    for message in consumer:
        data = message.value

        # Check for required fields
        if not all(k in data for k in ['bitcoin', 'ethereum', 'dogecoin']):
            print(f" Skipping malformed message: {data}")
            continue

        btc = data['bitcoin']['usd']
        eth = data['ethereum']['usd']
        doge = data['dogecoin']['usd']

        print(f"Consumed: BTC={btc}, ETH={eth}, DOGE={doge}")

        # Optional alert logic
        if btc > 150000 or eth > 10000:
            print("High value alert! (optional alert to Slack/Email here)")

        insert_sql = """
            INSERT INTO CRYPTO_PRICES (BITCOIN_USD, ETHEREUM_USD, DOGECOIN_USD)
            VALUES (%s, %s, %s)
        """
        cursor.execute(insert_sql, (btc, eth, doge))
        conn.commit()
        print("Data inserted into Snowflake")

except Exception as e:
    print(f"Error: {e}")
finally:
    cursor.close()
    conn.close()
    consumer.close()
    print(" Closed Snowflake and Kafka connections.")
