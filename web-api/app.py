import os
import time
import psycopg2
import redis
import pika
import json
from flask import Flask

app = Flask(__name__)
cache = redis.Redis(host=os.environ['REDIS_HOST'], port=6379)

def get_db_connection():
    # (This function remains the same)
    # ... (omitted for brevity)
    while True:
        try:
            conn = psycopg2.connect(
                host="db",
                database=os.environ['POSTGRES_DB'],
                user=os.environ['POSTGRES_USER'],
                password=os.environ['POSTGRES_PASSWORD']
            )
            return conn
        except psycopg2.OperationalError:
            print("Database not ready yet... waiting.")
            time.sleep(1)

# This is our main endpoint to simulate creating an order
@app.route('/')
def home():
    try:
        # Simulate creating an order ID
        order_id = int(time.time())
        
        # --- Connect to RabbitMQ and send a message ---
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='queue'))
        channel = connection.channel()
        channel.queue_declare(queue='orders')

        message = {
            'id': order_id,
            'customer_email': 'customer@example.com',
            'details': 'Order for one laptop'
        }
        
        channel.basic_publish(exchange='',
                              routing_key='orders',
                              body=json.dumps(message))
        
        connection.close()
        print(f" [web-api] Sent message for Order ID: {order_id}")
        
        return f"Order Placed Successfully! Order ID: {order_id}. Check your terminal logs to see the worker process it."

    except Exception as e:
        return f"An error occurred: {e}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
