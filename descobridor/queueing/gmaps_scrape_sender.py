import os
import sys
from typing import Dict, List
import pika
import json
from datetime import datetime, timedelta
import argparse

from descobridor.queueing.queues import gmaps_scrape_queue
from truby.db_connection import MongoConnection
from descobridor.queueing.constants import (
    GMAPS_SCRAPE_BATCH_SIZE, GMAPS_SCRAPE_KEY, TOPIC_EXCHANGE, GMAPS_SCRAPE_FREQ_D
)


def get_next_batch(language: str):
    """Get next batch of messages for the queue.
    query mongodb docs so that:
    1. it prioritizes places that haven't been scraped yet
    2. selects places with_data_id non-null
    3. returns {"place_id", "priority", "name", "data_id"}
    4. selects only places which were scraped more than GMAPS_SCRAPE_FREQ_D days ago
    or have a review_extr_ds_{language} equal to None
    this will require a new field in the db, definding desired scraping frequency
    we also need to have separate priority for gmaps scraping and serping
    
    :param language: language of the places to be scraped.
        eg 'en' or 'es'  
    """
    with MongoConnection("places") as db:
        time_field = f"review_extr_ds_{language.lower()}"
        cursor = db.collection.find(
            _scrape_conditions(language),
            {"place_id", "priority", "name", "data_id", time_field}
            ).sort("priority", -1).limit(GMAPS_SCRAPE_BATCH_SIZE)
        documents = list(cursor)
        [doc.pop("_id") for doc in documents]
        return documents
    
    
def _scrape_conditions(time_field: str):
    now = datetime.now()
    older_than = str((now - timedelta(days=GMAPS_SCRAPE_FREQ_D)).date())
    return {"data_id": {"$ne": None},
             "unscrapable": {"$ne": True},
             "$or": [
                 {time_field: {"$exists": False}},
                 {time_field: None},
                 {time_field: {"$lt": older_than}}
             ]
             }

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



def main(language: str):
    connection, channel, queue_name = gmaps_scrape_queue()
    next_batch = get_next_batch(language)
    append_to_queue(channel, next_batch)
    connection.close()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-l", "--language", required=True, help="Desired language of the reviews")
    args = vars(ap.parse_args())
    try:
        main(args["language"])
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
