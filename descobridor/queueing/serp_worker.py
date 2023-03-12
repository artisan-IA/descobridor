import sys
import os
import time
from datetime import datetime
from random import random
from pathlib import Path

scrptdir = Path("~/artesania/descobridor").expanduser()
os.chdir(scrptdir)
    

from descobridor.queueing.queues import serp_queue # noqa E402



def callback(ch, method, properties, body):
    print(f" [x] Received {body} at {datetime.now().strftime('%H:%M:%S')}")
    time.sleep(1)
    status = is_serp_limit_reached()
    print(f" [x] Done at {datetime.now().strftime('%H:%M:%S')}")
    if status:
        ch.basic_ack(delivery_tag = method.delivery_tag)
    else:
        # need some kind of negative acknolwedgement here
        time.sleep(1)
        print("No luck this time.")
        ch.basic_ack(delivery_tag = method.delivery_tag)
    
def is_serp_limit_reached():
    """Check if the limit of our SERP requests has been reached."""
    if random() > 0.8:
        return False
    else:
        return True
        


def main():
    channel, queue_name = serp_queue()
    # This tells RabbitMQ not to give more than one message to a worker at a time.
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)


    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
