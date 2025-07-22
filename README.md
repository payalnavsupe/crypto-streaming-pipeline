# Real-Time Crypto Streaming Pipeline using Kafka, Snowflake, Airflow, and dbt

1. Introduction: This project demonstrates the implementation of an automated streaming ELT pipeline using Kafka, Apache Airflow, Snowflake, and dbt. It simulates a real-time data ingestion system that fetches cryptocurrency prices from the CoinGecko API, streams them via Kafka, loads them into Snowflake, and performs light transformations using dbt.

2. Project Overview: The objective is to build a robust streaming pipeline that:

- Ingests live crypto price data
- Streams data using Kafka producer-consumer
- Loads it into Snowflake
- Transforms it using dbt
- Automates the workflow with Airflow
- Monitors the pipeline via Airflow UI
- This mimics real-world end-to-end data workflows used in fintech and trading analytics.

3. Key Features :

- Streaming Ingestion: Kafka Producer fetches real-time prices of Bitcoin, Ethereum, and Dogecoin from CoinGecko.
- Kafka Messaging: Producer sends JSON data to Kafka topic; Consumer reads and inserts into Snowflake.
- Data Warehousing: Snowflake is used to store both raw and transformed data.
- Transformations: dbt performs basic modeling and creates analytics-ready views.
- Automation: Airflow orchestrates the DAG to create tables, run dbt, and monitor flow.
- Monitoring: Airflow UI tracks DAG runs, task status, and logs.

4. Tech Stack :

- Kafka – Real-time data ingestion
- Apache Airflow – Orchestration & automation
- Snowflake – Cloud data warehouse
- dbt – SQL-based data transformation
- Python – Custom ETL & streaming scripts
- CoinGecko API – Crypto data source

5. Pipeline Architecture

[CoinGecko API]
       ↓
[Kafka Producer] → Kafka Topic → [Kafka Consumer]
                                      ↓
                            [Snowflake Raw Table]
                                      ↓
                                 [dbt Models]
                                      ↓
                        [Snowflake Final Views]
                                      ↓
                                [Airflow DAGs]

6. Folder Structure

CRYPTO_STREAMING/
├── airflow/                 # Airflow config and db
├── airflow_home/
│   └── dags/
│       └── crypto_streaming_pipeline.py
├── consumer/
│   ├── consumer.py          # Kafka consumer → Snowflake
│   └── Dockerfile (optional)
├── producer/
│   ├── producer.py          # Kafka producer → API
│   └── Dockerfile (optional)
├── crypto_dbt/              # dbt project
│   ├── models/
│   └── dbt_project.yml
├── requirements.txt
└── README.md

7.  Setup Instructions (Local Machine)

1) Kafka Setup
- Start Kafka and Zookeeper locally (via Docker or manually)
- bin/zookeeper-server-start.sh config/zookeeper.properties
- bin/kafka-server-start.sh config/server.properties

2) Create Kafka topic:
- bin/kafka-topics.sh --create --topic crypto-prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

3) Airflow Setup
Create virtual environment
python3 -m venv airflow_env  
source airflow_env/bin/activate  

4) Install dependencies
pip install -r requirements.txt  

5) Initialize Airflow
airflow db init  
airflow users create ...  

6) Start Airflow
airflow scheduler  
airflow webserver  

7) Snowflake Setup
- Create a database, schema, and warehouse manually.
- Set up a connection named snowflake_conn in Airflow with credentials.
- Optional: turn off OCSP certificate check (insecure mode) if needed locally.


8. Running the Pipeline

1) Start Kafka producer
python producer/producer.py
2) Start Kafka consumer
python consumer/consumer.py
3) Trigger DAG in Airflow UI
The DAG will:
- Create the CRYPTO_PRICES table
- Run dbt transformations

9. OUTCOMES:
- Real-time crypto data streaming from API to warehouse
- Snowflake stores both raw and transformed data
- dbt creates lightweight analytical views
- DAG automation with email alerts (optional setup)
- Monitoring via Airflow UI

10. Future Enhancements

- Add alerting logic (e.g. price thresholds → email/Slack)
- Add dashboard using Tableau or Streamlit
- Move producer/consumer into Dockerized microservices
- Use Kafka Connect for more scalable ingestion
- Implement dbt tests and documentation
