from datetime import datetime, timedelta
from airflow import models
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.python import PythonOperator
from google.cloud import pubsub_v1

# Your GCP and bucket details
BUCKET_NAME = 'online-payments2'
PREFIX = 'data_'
PROJECT_ID = 'mlrit2-460809'
TOPIC_ID = 'fraud-detection'

default_args = {
    'start_date': datetime(2025, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def publish_message_to_pubsub(**context):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    # Construct dynamic filename based on DAG run date
    execution_date = context['ds_nodash']  # e.g., 20250524
    file_name = f"{PREFIX}{execution_date}.csv"

    message_data = b'gcs-file-metadata'  # Optional message body
    attributes = {
        'bucket': BUCKET_NAME,
        'filename': file_name,
        'timestamp': context['ts'],
    }

    future = publisher.publish(topic_path, data=message_data, **attributes)
    message_id = future.result()
    print(f"Published message ID: {message_id}")

with models.DAG(
    dag_id='gcs_file_check_opfd',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    description='Check GCS bucket for new CSV files and publish metadata to Pub/Sub',
) as dag:

    wait_for_new_files = GCSObjectsWithPrefixExistenceSensor(
        task_id='wait_for_new_files',
        bucket=BUCKET_NAME,
        prefix=PREFIX,
        timeout=60,
        poke_interval=30,
        mode='poke',
    )

    notify_pubsub = PythonOperator(
        task_id='publish_to_pubsub',
        python_callable=publish_message_to_pubsub,
        provide_context=True,
    )

    wait_for_new_files >> notify_pubsub
