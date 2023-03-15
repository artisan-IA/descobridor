from typing import Tuple
import pika
from descobridor.queueing.constants import (
    DIRECT_EXCHANGE, TOPIC_EXCHANGE,
    SERP_QUEUE_NAME, SERP_QUEUE_MAX_LENGTH, SERP_QUEUE_MAX_PRIORITY,
    GMAPS_SCRAPE_QUEUE_MAX_LENGTH, GMAPS_SCRAPE_QUEUE_MAX_PRIORITY,
    GMAPS_SCRAPE_QUEUE_NAME
)


def serp_queue() -> Tuple[pika.BlockingConnection, pika.adapters.blocking_connection.BlockingChannel, str]:
    # publisher confirms
    """Connect to the queue."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()
    channel.exchange_declare(exchange=DIRECT_EXCHANGE, exchange_type='direct', durable=True)
    channel.queue_declare(
        queue=SERP_QUEUE_NAME, 
        durable=True,
        arguments={"x-max-priority": SERP_QUEUE_MAX_PRIORITY, 
                   'x-max-length': SERP_QUEUE_MAX_LENGTH, 
                   'x-overflow': 'reject-publish'}
        )
    channel.queue_bind(
            exchange=DIRECT_EXCHANGE, queue=SERP_QUEUE_NAME)
    channel.confirm_delivery()
    return connection, channel, SERP_QUEUE_NAME


def gmaps_scrape_queue():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()
    channel.exchange_declare(exchange=TOPIC_EXCHANGE, exchange_type='topic', durable=True)
    channel.queue_declare(
        queue=GMAPS_SCRAPE_QUEUE_NAME, 
        durable=True,
        arguments={"x-max-priority": GMAPS_SCRAPE_QUEUE_MAX_PRIORITY, 
                   'x-max-length': GMAPS_SCRAPE_QUEUE_MAX_LENGTH, 
                   'x-overflow': 'reject-publish'}
        )
