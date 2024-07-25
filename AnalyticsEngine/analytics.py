import os
import asyncio
import concurrent.futures
import threading
import time
import pickle
import redis
import logging
from flask import Flask, request
from google.cloud import pubsub_v1
from apscheduler.schedulers.background import BackgroundScheduler
import psycopg2
from psycopg2 import sql
from psycopg2 import pool

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Pub/Sub setup
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
input_subscription_path = subscriber.subscription_path('your-project-id', 'input-subscription')
output_topic_path = publisher.topic_path('your-project-id', 'output-topic')

# Redis setup with connection pooling
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_pool = redis.ConnectionPool(host=redis_host, port=6379, db=0)
redis_client = redis.Redis(connection_pool=redis_pool)

# Database connection pooling
db_host = os.getenv('DB_HOST', 'localhost')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_pool = psycopg2.pool.SimpleConnectionPool(1, 20,
                                             dbname=db_name,
                                             user=db_user,
                                             password=db_password,
                                             host=db_host,
                                             port="5432")

# Load the pickled ML model
with open('your_model.pkl', 'rb') as model_file:
    model = pickle.load(model_file)

# Thread pool for processing messages
executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

async def process_message(message):
    try:
        logging.info(f"Received message: {message.data}")
        # Your processing logic here
        await publish_result(message.data)
        # Acknowledge the message
        message.ack()
    except Exception as e:
        logging.error(f"Error processing message: {e}")

async def publish_result(data):
    try:
        publisher.publish(output_topic_path, data)
        logging.info(f"Published message to {output_topic_path}")
    except Exception as e:
        logging.error(f"Error publishing message: {e}")

def callback(message):
    asyncio.run_coroutine_threadsafe(process_message(message), asyncio.get_event_loop())

subscriber.subscribe(input_subscription_path, callback=callback)

# Scheduler setup
scheduler = BackgroundScheduler()

def run_nightly_job():
    conn = db_pool.getconn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, user_id FROM transaction_history WHERE category_id IS NULL")
        rows = cur.fetchall()
        for row in rows:
            transaction_id, user_id = row
            # Predict the category_id using the model
            category_id = model.predict([[transaction_id]])[0]
            # Update the database
            cur.execute(
                sql.SQL("UPDATE transaction_history SET category_id = %s WHERE id = %s"),
                [category_id, transaction_id]
            )
            # Update Redis cache
            redis_client.sadd(f"user:{user_id}", category_id)
        conn.commit()
        logging.info("Nightly job completed successfully")
    except Exception as e:
        logging.error(f"Error in nightly job: {e}")
    finally:
        cur.close()
        db_pool.putconn(conn)

scheduler.add_job(run_nightly_job, 'cron', hour=0)
scheduler.start()

@app.route('/')
def home():
    return "Pub/Sub Listener and Nightly Job Service"

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app.run(debug=True, use_reloader=False)
    loop.run_forever()
