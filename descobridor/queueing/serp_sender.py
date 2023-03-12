# cron job: every 10 minutes
# cron settings: */10 * * * *
import os
import sys
import pika
import json
from pathlib import Path

scrptdir = Path("~/artesania/descobridor").expanduser()
os.chdir(scrptdir)

from descobridor.queueing.constants import SERP_QUEUE_NAME, DIRECT_EXCHANGE, SERP_BATCH_SIZE # noqa E402
from descobridor.queueing.queues import serp_queue # noqa E402
from truby.db_connection import MongoConnection # noqa E402


def get_next_batch(): # TODO: implement
    """Get next batch of messages for the queue."""
    with MongoConnection("places") as db:
        cursor = db.collection.find(
            {"data_id": None}, 
            {"place_id", "priority", "name", "coords", "data_id"}
            ).sort("priority", -1).limit(SERP_BATCH_SIZE)
        documents = list(cursor)
        [doc.pop("_id") for doc in documents]
        return documents


def is_queue_available():
    """Check if there are messages in the queue."""
    # print(queue.method.message_count)
    ...
    return True
    
    
def append_to_queue(channel, next_batch):
    """Append messages to the queue."""
    for doc in next_batch:
        message = json.dumps(doc)
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
