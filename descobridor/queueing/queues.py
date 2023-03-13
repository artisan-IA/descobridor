from typing import Tuple
import pika
from descobridor.queueing.constants import SERP_QUEUE_NAME, DIRECT_EXCHANGE

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
        arguments={"x-max-priority": 10, 'x-max-length': 20, 'x-overflow': 'reject-publish'}
        )
    channel.queue_bind(
            exchange=DIRECT_EXCHANGE, queue=SERP_QUEUE_NAME)
    channel.confirm_delivery()
    return connection, channel, SERP_QUEUE_NAME
