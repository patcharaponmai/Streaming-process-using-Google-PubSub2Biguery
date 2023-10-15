import os
import sys
import json
import random
import configparser
import time
import uuid
from dotenv import load_dotenv
from datetime import datetime
from faker import Faker
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

def get_event_id():
    unique_id = uuid.uuid4()

    return str(unique_id)

def get_event():
    events = ["Cliked_item", "Click_item_description", "Purchase_item", "Review_item"]

    return random.choice(events)

def get_category():
    category = ["Food", "Clothes", "Eletronic", "Game"]

    return random.choice(category)

def get_name():

    return fake.name()

def get_item_id():

    return fake.sha1()


def get_item_quantity(eventname):
    if eventname == "Purchase_item":
        qty = random.randint(1,5)
    else:
        qty = 0

    return qty

def get_eventtime():

    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def main():

    print("======================================")
    print("====== Start Generate Mock Data ======")
    print("======================================")
    print()

    # Create a fully-qualified topic path
    topic_path = publisher.topic_path(project=project_id, topic=pubsub_topic)

    number = 0

    try:
        # Generate and publish mock data to Pub/Sub
        while True:

            delay_time = random.randint(2,3)
            time.sleep(delay_time)

            eventname = get_event()
            item_qty = get_item_quantity(eventname)

            event = {
                'No': number,
                'event_id': get_event_id(),
                'name': get_name(),
                'event_name': eventname,
                'category': get_category(),
                'item_id': get_item_id(),
                'item_quantity': item_qty,
                'event_time': get_eventtime(),
            }

            print(json.dumps(event))
            number+=1
            
            # Publish the mock data to the Pub/Sub topic
            try:
                publisher.publish(topic_path, json.dumps(event).encode('utf-8'))
            except Exception as e:
                print(f"Error cannot publish data: {e}")
                sys.exit(1)
        
    except KeyboardInterrupt as e:
        print()
        print("=======================================")
        print("===== Complete Generate Mock Data =====")
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
    except Exception as e:
        print(f"Error cannot get require parameters: {e}")
        sys.exit(1)

    # Initialize Pub/Sub Publisher client
    try:
        publisher = pubsub_v1.PublisherClient()
    except Exception as e:
        print(f"Error cannot create connection with Pub/Sub client: {e}")
        sys.exit(1)
    
    # Initialize the Faker library for generating mock data
    fake = Faker()

    main()