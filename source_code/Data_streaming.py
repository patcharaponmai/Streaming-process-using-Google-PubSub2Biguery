import sys
import configparser
import time
import json
from dotenv import load_dotenv
from google.cloud import bigquery, pubsub_v1
from google.cloud.exceptions import exceptions
from google.api_core.exceptions import AlreadyExists

def stream_to_bigquery(message, table):
    """
    Triggered by a message published on the Pub/Sub topic
    """
    try:
        data = json.loads(message.data.decode('utf-8'))

        # Load data into BigQuery
        client.insert_rows(table, [data])
        
        # Acknowledge the message to remove it from the subscription
        message.ack()

    except Exception as e:
        print(f"Error processing and streaming data to BigQuery: {e}")

def main():

    print("======================================")
    print("======== Start Streaming Data ========")
    print("======================================")
    print()

    # Create dataset
    try:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset = client.create_dataset(dataset, exists_ok=True)
    except AlreadyExists:
        print(f"Dataset '{dataset}' already exists.")
    except Exception as e:
        print(f"Error cannot create DataSet in BigQuery {e}")
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

    # Create table in BigQuery
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
        print(f"Table '{table_id}' already exists.")
    except exceptions.NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Table '{table_id}' has been created.")
    except Exception as e:
        print(f"Error creating BigQuery table: {e}")
        sys.exit(1)
    
    time.sleep(10)

    # Create a fully-qualified topic path
    topic_path = publisher.topic_path(project=project_id, topic=pubsub_topic)

    # Create a fully-qualified subscription path
    subscription_path = subscriber.subscription_path(project=project_id, subscription=pubsub_subscription)

    # Create Pub/Sub topic if it doesn't exist
    try:
        publisher.create_topic(name=topic_path)
        print(f"Pub/Sub Topic has been created")
    except AlreadyExists:
        print(f"Pub/Sub topic '{pubsub_topic}' already exists.")
    except Exception as e:
        print(f"Error creating Pub/Sub topic: {e}")
        sys.exit(1)

    time.sleep(10)

    # Create Pub/Sub subscription if it doesn't exist
    try:
        subscriber.create_subscription(name=subscription_path, topic=topic_path, ack_deadline_seconds=300)
        print(f"Pub/Sub Subscription has been created")
    except AlreadyExists:
        print(f"Pub/Sub subscription '{pubsub_subscription}' already exists.")
    except Exception as e:
        print(f"Error creating Pub/Sub subscription: {e}")
        sys.exit(1)

    def callback(message):
        stream_to_bigquery(message, table)

    # Consuming data from Pub/Sub topic and ingest into BigQuery
    print(f"Listening for messages on {subscription_path}..")
    subscriber.subscribe(subscription=subscription_path, callback=callback)
    
    # Keep the script running to continue processing messages
    try:
        while True:
            time.sleep(30)  # You can adjust the sleep interval as needed
            print("Consuming message is in progress ...")
    except KeyboardInterrupt:
        print()
        print("=======================================")
        print("======= Complete Streaming Data =======")
        print("=======================================")


######################################
############ MAIN PROGRAM ############
######################################

if __name__ == '__main__':

    # Load environment
    load_dotenv()

    # Initialize the configuration parser
    config = configparser.ConfigParser()

    # Read configuration file
    config.read("./config.ini")

    try:
        project_id = config.get('PROJ_CONF', 'PROJ_ID')
        pubsub_topic = config.get('PROJ_CONF', 'PUBSUB_TOPIC_NAME')
        pubsub_subscription = config.get('PROJ_CONF', 'PUBSUB_SUBSCRIPTION_NAME')
        dataset_id = config.get('PROJ_CONF', 'DATASET_NAME')
        table_id = config.get('PROJ_CONF', 'TABLE_NAME')
    except Exception as e:
        print(f"Error cannot get require parameters: {e}")
        sys.exit(1)

    # Initialize Pub/Sub Publisher and Subscriber client
    try:
        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()
    except Exception as e:
        print(f"Error cannot create connection with Pub/Sub client: {e}")
        sys.exit(1)

    # Initialize the BigQuery client
    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Error cannot create connection with BigQuery client: {e}")
        sys.exit(1)

    main()