import sys
import os
import time
from datetime import datetime
import json

    

from descobridor.queueing.queues import serp_queue, bind_client_to_serp_queue
from descobridor.queueing.constants import SERP_QUEUE_NAME
from descobridor.discovery.serp_api import serp_search_place, OutOfRequestsError
from descobridor.queueing.change_serpjob_freq import postpone_job
from descobridor.the_logger import logger


def callback(ch, method, properties, body):
    logger.info(f" [x] Received {body} at {datetime.now().strftime('%H:%M:%S')}")
    time.sleep(1)
    record = json.loads(body)
    try:
        _ = serp_search_place(record["place_id"], record["name"], record["coords"], use_cache=True)
    except OutOfRequestsError:
        logger.error("Out of requests, postponing job")
        postpone_job()
        ch.queue_purge(queue=SERP_QUEUE_NAME)
        ch.basic_ack(delivery_tag = method.delivery_tag)
    else:
        logger.info(f" [x] Done at {datetime.now().strftime('%H:%M:%S')}")
        ch.basic_ack(delivery_tag = method.delivery_tag)


def main():
    connection, channel, queue_name = serp_queue()
    bind_client_to_serp_queue(channel)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)


    logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.warning('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
