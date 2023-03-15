import os
import sys

from descobridor.queueing.queues import gmaps_scrape_queue


def get_next_batch():
    ...
    

def append_to_queue(channel, next_batch):
    ...
    

def main():
    connection, channel, queue_name = gmaps_scrape_queue()
    next_batch = get_next_batch()
    append_to_queue(channel, next_batch)
    connection.close()
    
    
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
