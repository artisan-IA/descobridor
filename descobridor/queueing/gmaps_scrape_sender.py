import os
import sys
from typing import Dict, List, Any
import pika
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

from descobridor.queueing.queues import gmaps_scrape_queue
from truby.db_connection import MongoConnection
from descobridor.queueing.constants import (
    GMAPS_SCRAPE_BATCH_SIZE, GMAPS_SCRAPE_KEY, TOPIC_EXCHANGE, GMAPS_SCRAPE_FREQ_D,
    GMAPS_SCRAPER_INTERFACE
)
from descobridor.helpers import choose_language_domain



load_dotenv()


def loc_last_scraped(language: str) -> str:
    return f"review_extr_ds_{language}"
    
    
def prepare_request(doc: Dict[str, Any], language: str, domain: str) -> Dict:
    """Add language (as a 2 letter code) and country domain fields 
    to the document."""
    doc = doc.copy()
    doc.pop('_id')
    doc['language'] = language
    doc['country_domain'] = domain
    doc['last_scraped'] = doc[loc_last_scraped(language)]
    doc.drop(loc_last_scraped(language))
    assert set(doc.keys()) == GMAPS_SCRAPER_INTERFACE
    return doc


def scrape_conditions(last_scraped: str):
    """
    out of the vast number of places in the db, we want to select only those
    that have data_id, and have not been scraped 
    in the last GMAPS_SCRAPE_FREQ_D days
    """
    now = datetime.now()
    older_than = str((now - timedelta(days=GMAPS_SCRAPE_FREQ_D)).date())
    return {"data_id": {"$ne": None},
             "unscrapable": {"$ne": True},
             "$or": [
                 {last_scraped: {"$exists": False}},
                 {last_scraped: None},
                 {last_scraped: {"$lt": older_than}}
             ]
             }


# main functions


def get_next_batch():
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
    loc = choose_language_domain(os.environ["country"])
    with MongoConnection("places") as db:
        last_scraped = loc_last_scraped(loc['language'])
        cursor = db.collection.find(
            scrape_conditions(last_scraped),
            {"place_id", "priority", "name", "data_id", last_scraped}
            ).sort("priority", -1).limit(GMAPS_SCRAPE_BATCH_SIZE)
        documents = list(cursor)
        return [prepare_request(doc, loc['language'], loc['domain']) 
                for doc in documents]
    
    
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
