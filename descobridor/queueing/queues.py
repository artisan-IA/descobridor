from typing import Tuple, Optional
import pika
import os
from descobridor.queueing.constants import (
    DIRECT_EXCHANGE, TOPIC_EXCHANGE,
    SERP_QUEUE_NAME, SERP_QUEUE_MAX_LENGTH, SERP_QUEUE_MAX_PRIORITY,
    GMAPS_SCRAPE_QUEUE_MAX_PRIORITY,
    GMAPS_SCRAPE_KEY
)


def get_credentials() -> Tuple[str, str]:
    """Get credentials from environment variables."""
    user = os.environ.get('rabbitmq_user')
    passwd = os.environ.get('rabbitmq_pass')
    return pika.PlainCredentials(user, passwd)


def get_auth_connection():
    credentials = get_credentials()
    parameters = pika.ConnectionParameters(
        os.environ["rabbitmq_host"], 
        credentials=credentials,
        heartbeat=0
        )
    return pika.BlockingConnection(parameters)


def serp_queue() -> Tuple[pika.BlockingConnection, pika.adapters.blocking_connection.BlockingChannel, str]:
    # publisher confirms
    """Connect to the queue."""
    connection = get_auth_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange=DIRECT_EXCHANGE, exchange_type='direct', durable=True)
    channel.queue_declare(
        queue=SERP_QUEUE_NAME, 
        durable=True,
        arguments={"x-max-priority": SERP_QUEUE_MAX_PRIORITY, 
                   'x-max-length': SERP_QUEUE_MAX_LENGTH, 
                   'x-overflow': 'reject-publish'}
        )
    channel.confirm_delivery()
    return connection, channel, SERP_QUEUE_NAME


def bind_client_to_serp_queue(
    channel: pika.adapters.blocking_connection.BlockingChannel, 
    ):
    channel.queue_bind(
        exchange=DIRECT_EXCHANGE, 
        queue=SERP_QUEUE_NAME
        )


def gmaps_scrape_queue():
    connection = get_auth_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange=TOPIC_EXCHANGE, exchange_type='topic', durable=True)
    channel.queue_declare(
        queue=GMAPS_SCRAPE_KEY, 
        durable=True,
        arguments={'x-max-priority': GMAPS_SCRAPE_QUEUE_MAX_PRIORITY, 
                   'x-max-length': 10, 
                   'x-overflow': 'reject-publish'}
        )
    channel.confirm_delivery()
    return connection, channel, GMAPS_SCRAPE_KEY
    
    
def bind_client_to_gmaps_scrape(
    channel: pika.adapters.blocking_connection.BlockingChannel, 
    routing_key: Optional[str] = GMAPS_SCRAPE_KEY
    ):
    channel.queue_bind(
        exchange=TOPIC_EXCHANGE, 
        queue=GMAPS_SCRAPE_KEY, 
        routing_key=routing_key
        )
    
