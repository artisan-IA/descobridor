# cron job: every 10 minutes
# cron settings: */10 * * * *
import os
import sys
import pika
from descobridor.queueing.constants import SERP_QUEUE_NAME, DIRECT_EXCHANGE
from descobridor.queueing.queues import serp_queue
# from truby.db_connection import MongoConnection


def get_next_batch(): # TODO: implement
    """Get next batch of messages for the queue."""
    return [f"id={i}" for i in range(10)]


def is_queue_available():
    """Check if there are messages in the queue."""
    # print(queue.method.message_count)
    ...
    return True


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
            exchange=DIRECT_EXCHANGE,
            routing_key=SERP_QUEUE_NAME,
            body=message,
            mandatory=True,
            properties=pika.BasicProperties(
                delivery_mode = pika.spec.PERSISTENT_DELIVERY_MODE
        ))
        
        
def main():
    channel, queue_name = serp_queue()
    next_batch = get_next_batch()
    if is_queue_available():
        append_to_queue(channel, next_batch)
        
        
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
