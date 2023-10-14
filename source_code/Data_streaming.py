import sys
import configparser
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import pubsub_v1

def stream_to_bigquery():
    pass

if __name__ == '__main__':

    # Load environment
    load_dotenv()

    # Initialize the configuration parser
    config = configparser.ConfigParser()

    # Read configuration file
    config.read("./config.ini")

    try:
        project_id = config.get('PROJ_CONF', 'PROJ_ID')
        subscription_name = config.get('PROJ_CONF', 'PUBSUB_TOPIC_NAME')
        dataset_id = config.get('PROJ_CONF', 'DATASET_NAME')
        table_id = config.get('PROJ_CONF', 'TABLE_NAME')

    except Exception as e:
        print("Error: Cannot get a require parameters.")
        print(e)
        sys.exit(1)

    # Initialize the BigQuery client
    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        print("Error: Cannot create connection with BigQuery client")
        print(e)
        sys.exit(1)

    # Create dataset
    try:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset = client.create_dataset(dataset, exists_ok=True)
    except Exception as e:
        print("Error: Cannot create DataSet in BigQuery")
        print(e)
        sys.exit(1)

    # Define table schema
    schema = [
        bigquery.SchemaField('No', 'STRING'),
        bigquery.SchemaField('event_id', 'STRING'),
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('event_name', 'STRING'),
        bigquery.SchemaField('category', 'STRING'),
        bigquery.SchemaField('item_id', 'STRING'),
        bigquery.SchemaField('item_quantity', 'INT64'),
        bigquery.SchemaField('event_time', 'TIMESTAMP'),
    ]

    # Create table
    try:
        table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}", schema=schema)
        table = client.create_table(table)
    except Exception as e:
        print("Error: Cannot create Table in BigQuery")
        print(e)
        sys.exit(1)

    # Initialize the Subscription client
    try:
        subscriber = pubsub_v1.SubscriberClient()
    except Exception as e:
        print("Error: Cannot create connection with BigQuery client")
        print(e)
        sys.exit(1)

    # # Define subscription path to interact with Pub/Sub topic
    # subscription_path = subscriber.subscription_path(project=project_id, subscription=subscription_name)

    # # Consuming data from Pub/Sub topic and ingest into BigQuery
    # subscriber.subscribe(subscription=subscription_path, callback=stream_to_bigquery)
