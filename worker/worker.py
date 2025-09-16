import pika
import time
import os

def connect_to_rabbitmq():
    # This function tries to connect to RabbitMQ, retrying if it fails.
    while True:
        try:
            print("Connecting to RabbitMQ...")
            # Use the service name 'queue' as the host
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='queue'))
            print("Connection to RabbitMQ successful!")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ not ready yet, waiting...")
            time.sleep(2) # Wait for 2 seconds before retrying

def callback(ch, method, properties, body):
    # This function processes messages (same as before)
    print(f"--- [Worker] Received a new order message: {body.decode()} ---")
    print(f"[Worker] Starting PDF invoice generation...")
    time.sleep(5)
    print(f"[Worker] ...Finished PDF invoice generation.")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"--- [Worker] Job finished. Waiting for next message. ---")

# --- Main execution block ---
connection = connect_to_rabbitmq()
channel = connection.channel()
channel.queue_declare(queue='orders')
channel.basic_consume(queue='orders', on_message_callback=callback)

print('[*] Waiting for messages in the "orders" queue. To exit press CTRL+C')
channel.start_consuming()
