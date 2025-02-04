from google.cloud import pubsub_v1
import json
import time

def publish_message(project_id, topic_id, message_data):

  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project_id, topic_id)

  try:
    if isinstance(message_data, dict):
        message_data = json.dumps(message_data).encode("utf-8") # Serialize to JSON if dict
    elif isinstance(message_data, str):
      message_data = message_data.encode("utf-8") # Encode strings to bytes
    else:
      raise ValueError("Message data must be a dictionary or string")

    future = publisher.publish(topic_path, data=message_data)
    message_id = future.result()

    print(f"Published message with ID: {message_id}")

  except Exception as e:
    print(f"Error publishing message: {e}")

ts = int(time.time() * 1000000)
# print(ts)
# print(1738595374148000)

project = "qr-code-decideomtour-scanflow"
# topic = "pubtest"
topic = "scanflow-scan-event-topic"
# msg = json.dumps({
#     "surname": "Frank",
#     "name": "Robert",
#     "email": "frob@decideom.com",
#     "company": "Decideom",
#     "city": "Paris",
#     "location": "ATL 1",
#     "scan_time": ts
# })
# print(msg)
import pandas as pd
df = pd.read_excel("avro_data_clean.xlsx")
for i , r in df.iterrows():
  if i == 0: 
    continue

  data = r.to_dict()
  msg = json.dumps(data)
  publish_message(project, topic, msg)
  # break

