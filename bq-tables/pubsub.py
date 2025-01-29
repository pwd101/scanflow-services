from google.cloud import pubsub_v1
import json

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


project = ""
topic = ""
msg = ""
publish_message(project, topic, msg)