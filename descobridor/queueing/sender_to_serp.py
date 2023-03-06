# cron job: every 10 minutes
# cron settings: */10 * * * *
import pika
from descobridor.queueing.constants import SERP_QUEUE_NAME


def connect_to_queue():
    # publisher confurms
    # quorum queue
    """Connect to the queue."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()
    channel.queue_declare(queue=SERP_QUEUE_NAME, durable=True)
    return channel


def get_next_batch(): # TODO: implement
    """Get next batch of messages for the queue."""
    return [{'id': i} for i in range(10)]


def check_queue_status():
    """Check if there are messages in the queue."""
    ...
    
    
def check_if_serp_limit_reached():
    """Check if the limit of our SERP requests has been reached."""
    return False


def send_alret():
    """Send an alert to the admin."""
    ...
    
    
def go_to_sleep():
    """Stop all the activity, util the new month starts.
    Will take rewriting the cron job.
    """
    ...
    
    
def append_to_queue(channel, next_batch):
    """Append messages to the queue."""
    for message in next_batch:
        channel.basic_publish(
            exchange='',
            routing_key=SERP_QUEUE_NAME,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode = pika.spec.PERSISTENT_DELIVERY_MODE
        ))
