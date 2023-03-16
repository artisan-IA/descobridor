import os
import sys
import redis
import time
import json

from descobridor.queueing.queues import gmaps_scrape_queue, bind_client_to_gmaps_scrape
from descobridor.discovery.read_raw_reviews import extract_all_reviews # noqa F401


def ensure_vpn_freshness():
    """
    checks how old the curren vpn connection is
    if it's older than a specified time, 
    it will connect to a different one available in redis
    """
    redis_client = redis.Redis(host='localhost', port=6379, db=0) # noqa F841
    # check how old the current vpn connection is
    # if it's older than a specified time,
    # connect to a different one available in redis
    # if there are no more vpn connections available,
    # wait until there are
    print(' [*] Ensuring vpn freshness')
    time.sleep(1)
    print(' [v] So fresh!')
    

def callback(ch, method, properties, body):
    ensure_vpn_freshness()
    gmaps_entry = json.loads(body)
    #extract_all_reviews(gmaps_entry)
    print(" [x] Received %r" % gmaps_entry)
    time.sleep(1)
    print(" [x] Done")
    
    
def main():
    connection, channel, queue_name = gmaps_scrape_queue()
    bind_client_to_gmaps_scrape(channel)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    # we need to consume from a specific topic, not queue. gotta figure out this bit


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

