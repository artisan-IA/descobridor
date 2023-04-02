import os
import uuid
from typing import Dict, Any
import pika
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

from descobridor.queueing.queues import get_auth_connection
from truby.db_connection import MongoConnection
from descobridor.queueing.constants import (
    GMAPS_SCRAPE_KEY, GMAPS_SCRAPE_FREQ_D,
    GMAPS_SCRAPER_INTERFACE
)
from descobridor.helpers import get_localization
from descobridor.the_logger import logger


load_dotenv()



class GmapsClient:
    def __init__(self) -> None:
        self.connection = get_auth_connection()
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        self.response = None
        self.corr_id = None
        
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def send_request(self):
        request = self.get_request()
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=GMAPS_SCRAPE_KEY,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(request))
        self.connection.process_data_events(time_limit=None)
        return int(self.response)
    
    def get_request(self):
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
        loc = get_localization(os.environ["country"])
        logger.info("Getting next batch of messages for gmaps scrape queue...")
        with MongoConnection("places") as db:
            last_scraped = self.loc_last_scraped(loc['language'])
            doc = db.collection.find_one(
                self.scrape_conditions(last_scraped),
                {"place_id", "priority", "name", "data_id", last_scraped}
                ).sort("priority", -1)

            return self.prepare_request(doc, loc['language'], loc['domain']) 

    @staticmethod
    def loc_last_scraped(language: str) -> str:
        return f"review_extr_ds_{language}"
        
        
    def prepare_request(self, doc: Dict[str, Any], language: str, domain: str) -> Dict:
        """Add language (as a 2 letter code) and country domain fields 
        to the document."""
        doc = doc.copy()
        doc.pop('_id')
        doc['language'] = language
        doc['country_domain'] = domain
        doc['last_scraped'] = doc[self.loc_last_scraped(language)]
        doc.pop(self.loc_last_scraped(language))
        assert set(doc.keys()) == GMAPS_SCRAPER_INTERFACE
        return doc

    @staticmethod
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





