import os
from google.cloud import pubsub_v1

credentials_path = "../AnalyticsEngine/lloyds-hack-grp-43-4e780060bdd5.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
publisher = pubsub_v1.PublisherClient()
topic_path = "projects/lloyds-hack-grp-43/topics/askR"
data = "A message from a Analytics"
data = data.encode('utf-8')
future = publisher.publish(topic_path, data)
print("published message id", future.result())