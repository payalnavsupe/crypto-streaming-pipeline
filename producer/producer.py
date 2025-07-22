import time
import json
import requests
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# 🔧 Kafka Configuration
KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'crypto-prices'

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# Retry Kafka connection
for i in range(10):
    try:
        log.info(f" Attempt {i+1}: Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVER}...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        log.info("Connected to Kafka!")
        break
    except NoBrokersAvailable:
        log.warning(" Kafka broker not available yet, retrying in 5 seconds...")
        time.sleep(5)
else:
    raise Exception(" Could not connect to Kafka after multiple attempts.")

# CoinGecko API endpoint
API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dogecoin&vs_currencies=usd"

# Continuous Streaming Loop
fail_count = 0

while True:
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        crypto_prices = response.json()

        # 📨 Send to Kafka
        producer.send(TOPIC_NAME, crypto_prices)
        log.info(f"Sent to Kafka: {crypto_prices}")
        fail_count = 0  # reset failure count on success

    except requests.exceptions.RequestException as e:
        log.error(f"API request failed: {e}")
        fail_count += 1
        if fail_count >= 3:
            log.warning(" ALERT: API failed 3 times in a row! Possible API downtime or network issue.")

    time.sleep(10)  # Delay between each fetch
