from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging
import requests
import subprocess
import os
from airflow.exceptions import AirflowException

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='crypto_streaming_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

def create_crypto_table():
    conn = None
    cursor = None
    try:
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CRYPTO_PRICES (
                id INT AUTOINCREMENT,
                bitcoin_usd FLOAT,
                ethereum_usd FLOAT,
                dogecoin_usd FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info("Table created successfully.")
    except Exception as e:
        logging.error(f"Table creation failed: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_crypto_data():
    url = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dogecoin&vs_currencies=usd'
    response = requests.get(url)
    data = response.json()

    btc_price = data['bitcoin']['usd']
    eth_price = data['ethereum']['usd']
    doge_price = data['dogecoin']['usd']

    logging.info(f"Prices - BTC: {btc_price}, ETH: {eth_price}, DOGE: {doge_price}")

    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO CRYPTO_PRICES (bitcoin_usd, ethereum_usd, dogecoin_usd)
            VALUES (%s, %s, %s)
        """, (btc_price, eth_price, doge_price))
        logging.info("Data inserted successfully.")
    except Exception as e:
        logging.error(f"Failed to insert data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def run_dbt_transformation():
    dbt_project_dir = "/Users/payal/Documents/Studies/Data Analyst/Projects/Crypto_Streaming/crypto_dbt"
    logging.info("Running dbt transformation...")
    result = subprocess.run(["dbt", "run", "--project-dir", dbt_project_dir], capture_output=True, text=True)

    if result.returncode != 0:
        logging.error(f"dbt failed: {result.stderr}")
        raise Exception("dbt transformation failed")
    logging.info("dbt transformation completed.")
    logging.info(result.stdout)




create_table_task = PythonOperator(
    task_id='create_crypto_table',
    python_callable=create_crypto_table,
    dag=dag,
)

fetch_data_task = PythonOperator(
    task_id='fetch_crypto_data',
    python_callable=fetch_crypto_data,
    dag=dag,
)

run_dbt_task = PythonOperator(
    task_id='run_dbt_transformation',
    python_callable=run_dbt_transformation,
    dag=dag,
)

create_table_task >> fetch_data_task >> run_dbt_task 