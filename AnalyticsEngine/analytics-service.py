import os
import asyncio
import concurrent.futures
import threading
import time
import random
import pickle
import redis
import logging
from flask import Flask, request
from google.cloud import pubsub_v1
from apscheduler.schedulers.background import BackgroundScheduler
import pg8000
import json

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Pub/Sub setup
credentials_path = "lloyds-hack-grp-43-e35386f27e3f.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
input_subscription_path = 'projects/lloyds-hack-grp-43/subscriptions/askAnalytics-sub'
output_topic_path = 'projects/lloyds-hack-grp-43/topics/askRC'

# Redis setup with connection pooling
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_pool = redis.ConnectionPool(host=redis_host, port=6379, db=0)
redis_client = redis.Redis(connection_pool=redis_pool)


# Database fetch
def getconn():
    try:
        conn = pg8000.connect(
            user="postgres",
            password="password",
            database="production",
            host="10.72.197.3",
            port=5432
        )
        return conn
    except Exception as e:
        print(f"Connection error: {e}")
        return None


# Load the pickled ML model
with open('your_model.pkl', 'rb') as model_file:
    model = pickle.load(model_file)


# Thread pool for processing messages
executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)


def process_message(message):
    try:
        logging.info(f"Received message: {message.data}")
        # Extract user_id from the message data (assuming the message is in JSON format)
        serial = json.loads(message.data)
        user_id = serial.get('userId')

        if user_id:
            # Retrieve all categories for the given user_id
            user_categories = []
            for category_id in redis_client.smembers("category_ids"):  # Assuming category IDs are from 1 to 10
                cat_id = category_id.decode('utf-8')
                if redis_client.sismember(f"category:{cat_id}", user_id):
                    user_categories.append(cat_id)

            logging.info(f"User {user_id} belongs to categories: {user_categories}")

            if user_categories:
                # Select a random category from the user's categories
                selected_category = random.choice(user_categories)
                logging.info(f"Selected category {selected_category} for user {user_id}")
                message_mapper = {
                    "category_id": selected_category
                }
                # Publish the selected category
                publish_result(message_mapper)

        # Acknowledge the message
        message.ack()
    except Exception as e:
        logging.error(f"Error processing message: {e}")


def publish_result(data):
    try:
        data_dumped = json.dumps(data).encode('utf-8')
        publisher.publish(output_topic_path, data_dumped)
        logging.info(f"Published message to {output_topic_path}")
    except Exception as e:
        logging.error(f"Error publishing message: {e}")


def callback(message):
    threading.Thread(target=process_message, args=(message,)).start()


subscriber.subscribe(input_subscription_path, callback=callback)

# Scheduler setup
scheduler = BackgroundScheduler()


def run_nightly_job():
    conn = getconn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT transaction_id, user_id, transaction_tag, city, amount "
                    "FROM transactions WHERE category_id IS NULL;")
        rows = cur.fetchall()
        category_ids_list = model.predict(rows)
        for itr, cat in enumerate(category_ids_list):
            cur.execute("SELECT DISTINCT category_id from categories where cat_name=%s", [cat])
            row = cur.fetchone()
            redis_client.sadd("category_ids", str(row[0]))
            transaction_id = rows[itr][0]
            # Predict the category_id using the model
            # Update the database
            cur.execute("UPDATE transactions SET category_id = %s WHERE transaction_id = %s",
                [row[0], transaction_id]
            )
            conn.commit()
            redis_client.sadd(f"category:{str(row[0])}", str(rows[itr][1]))
        logging.info("Nightly job completed successfully")
    except Exception as e:
        logging.error(f"Error in nightly job: {e}")
    finally:
        cur.close()


scheduler.add_job(run_nightly_job, 'interval', minutes=10)
scheduler.start()


@app.route('/')
def home():
    return "Pub/Sub Listener and Nightly Job Service"


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app.run(debug=True, use_reloader=False)
    loop.run_forever()
