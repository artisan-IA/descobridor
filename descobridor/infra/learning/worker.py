import pika, sys, os
import time
from datetime import datetime

QUEUE_NAME = 'task_queue'

def callback(ch, method, properties, body):
        print(f" [x] Received {body} at {datetime.now().strftime('%H:%M:%S')}")
        time.sleep(body.count(b'.'))
        print(f" [x] Done at {datetime.now().strftime('%H:%M:%S')}")
        ch.basic_ack(delivery_tag = method.delivery_tag)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # This tells RabbitMQ not to give more than one message to a worker at a time.
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)

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
