import os
import sys
from typing import Dict, List
import pika
import json

from descobridor.queueing.queues import gmaps_scrape_queue
from truby.db_connection import MongoConnection
from descobridor.queueing.constants import SERP_BATCH_SIZE, GMAPS_SCRAPE_KEY, TOPIC_EXCHANGE


def get_next_batch():
    """Get next batch of messages for the queue."""
    # query mongodb docs so that:
    # 1. it prioritizes places that haven't been scraped yet
    # 2. selects places with_data_id non-null
    # 3. returns {"place_id", "priority", "name", "coords", "data_id"}
    # 4. selects only places which weren't scaped for a given perion of time
    #    this will require a new field in the db, definding desired scraping frequency
    # we also need to have separate priority for gmaps scraping and serping
    with MongoConnection("places") as db:
        cursor = db.collection.find(
            {"data_id": {"$ne": None}, 
             "unscrapable": {"$ne": True},
             # "scrape_time": {"$lt": 10} # this gotta be fixed
             }, 
            {"place_id", "priority", "name", "data_id"}
            ).sort("priority", -1).limit(SERP_BATCH_SIZE)
        documents = list(cursor)
        [doc.pop("_id") for doc in documents]
        print(f"Got {len(documents)} documents")
        return documents
    

def append_to_queue(channel: pika.adapters.blocking_connection.BlockingChannel, next_batch: List[Dict]):
    """Append messages to the queue."""
    for doc in next_batch:
        message = json.dumps(doc) # not entirely, worker expects 'gmaps_entry' key
        channel.basic_publish(
            exchange=TOPIC_EXCHANGE,
            routing_key=GMAPS_SCRAPE_KEY,
            body=message,
            mandatory=True,
            properties=pika.BasicProperties(
                delivery_mode = pika.spec.PERSISTENT_DELIVERY_MODE
        ))
        print(f"Sent {doc['place_id']}")
        
    

def main():
    connection, channel, queue_name = gmaps_scrape_queue()
    next_batch = get_next_batch()
    append_to_queue(channel, next_batch)
    connection.close()
    
    
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
