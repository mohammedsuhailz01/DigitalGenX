import os
from google.cloud import pubsub_v1

credentials_path = "../AnalyticsEngine/lloyds-hack-grp-43-4e780060bdd5.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
timeout = 5.0
subscriber = pubsub_v1.SubscriberClient()
subscription_path = "projects/lloyds-hack-grp-43/subscriptions/askAnalytics-sub"

def callback(message):
    print('Received message:', message)
    print('data: ',message.data)
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print("Listening for messages on ", subscription_path)

with subscriber:
    try:
        # streaming_pull_future.result(timeout = timeout)
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()