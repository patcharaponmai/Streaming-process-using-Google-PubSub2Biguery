from google.cloud import pubsub_v1
from google.cloud import bigquery
import psycopg2

# Google Cloud project configuration
project_id = 'your-gcp-project'
subscription_name = 'your-pubsub-subscription'
dataset_id = 'your-dataset'
table_id = 'your-bigquery-table'

# PostgreSQL database connection parameters
db_params = {
    'dbname': 'your_db_name',
    'user': 'your_db_user',
    'password': 'your_db_password',
    'host': 'your_db_host',
    'port': 'your_db_port',
}

# Create a Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Initialize a BigQuery client
bq_client = bigquery.Client(project=project_id)

# Create a BigQuery dataset if it doesn't exist
dataset_ref = bq_client.dataset(dataset_id)
try:
    bq_client.create_dataset(dataset_ref)
except Exception:
    pass

# Create a BigQuery table if it doesn't exist
table_ref = dataset_ref.table(table_id)
table = bigquery.Table(table_ref)
schema = [
    bigquery.SchemaField('name', 'STRING'),
    bigquery.SchemaField('age', 'INTEGER'),
]
table.schema = schema
try:
    bq_client.create_table(table)
except Exception:
    pass

# PostgreSQL data generation and Pub/Sub publishing script (simplified)
def generate_mock_data_and_publish_to_pubsub():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, 'your-pubsub-topic')

    while True:
        # Mock data generation in PostgreSQL
        name = 'John Doe'
        age = 30

        # Insert data into PostgreSQL
        cursor.execute("INSERT INTO your_postgresql_table (name, age) VALUES (%s, %s)", (name, age))
        conn.commit()

        # Publish the data to Pub/Sub
        data = f"Name: {name}, Age: {age}"
        publisher.publish(topic_path, data=data.encode('utf-8'))

# Pub/Sub message processing and BigQuery insertion
def process_pubsub_message(message):
    # Message processing logic (e.g., data transformation)
    data = message.data.decode('utf-8').split(', ')
    name = data[0].split(': ')[1]
    age = int(data[1].split(': ')[1])

    # Insert the data into BigQuery
    row = (name, age)
    errors = bq_client.insert_rows_json(table, [row])

    if errors:
        print(errors)

    message.ack()

# Main loop
if __name__ == '__main__':
    # Continuously generate data and publish to Pub/Sub
    generate_mock_data_and_publish_to_pubsub()

    # Subscribe to Pub/Sub and process messages
    subscriber.subscribe(subscription_path, callback=process_pubsub_message)
