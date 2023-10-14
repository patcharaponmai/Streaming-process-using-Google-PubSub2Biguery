import os
from google.cloud import bigquery
from google.cloud import pubsub_v1

# Create dataset and table (
# ==================================== 
# from google.cloud import bigquery

# # Set your project ID and dataset ID
# project_id = 'your-project-id'
# dataset_id = 'your-dataset-id'


# # Initialize the BigQuery client
# client = bigquery.Client(project=project_id)

# # Define the dataset ID and other optional settings
# dataset_id = 'your-dataset-id'
# dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")

# # Create the dataset
# dataset = client.create_dataset(dataset, exists_ok=True)

# print(f"Dataset {dataset.dataset_id} created.")

# # Define the table ID and its schema
# table_id = 'your-table-id'
# schema = [
#     bigquery.SchemaField('field1', 'STRING'),
#     bigquery.SchemaField('field2', 'INTEGER'),
#     # Add more fields as needed
# ]

# # Define the table
# table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}", schema=schema)

# # Create the table
# table = client.create_table(table)

# print(f"Table {table.table_id} created.")
# ====================================

# Set up the BigQuery client
bigquery_client = bigquery.Client(project=project_id)

# Set up the Pub/Sub client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def stream_to_bigquery(data, context):
    # Triggered by a message on the Pub/Sub topic

    # Parse the message JSON
    message_data = data['data']
    message_attributes = data['attributes']

    # Insert the data into BigQuery
    # You may need to adjust this code to match the structure of your data
    # This example assumes the following keys in your message: event_id, name, event_name, category, item_id, item_quantity, event_time
    row = {
        'event_id': message_attributes['event_id'],
        'name': message_attributes['name'],
        'event_name': message_attributes['event_name'],
        'category': message_attributes['category'],
        'item_id': message_attributes['item_id'],
        'item_quantity': message_attributes['item_quantity'],
        'event_time': message_attributes['event_time'],
    }
    
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    table = bigquery_client.get_table(table_ref)
    errors = bigquery_client.insert_rows_json(table, [row])

    if not errors:
        print(f'Successfully streamed data to BigQuery: {message_data}')
    else:
        print(f'Error streaming data to BigQuery: {errors}')

# Set up the Pub/Sub subscription
subscriber.subscribe(subscription_path, callback=stream_to_bigquery)

# This script will keep running, listening for new messages and streaming them to BigQuery.
# You can use Ctrl+C to stop the script.
