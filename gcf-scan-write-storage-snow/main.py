import base64
import json
import avro.schema
from google.cloud import storage
import functions_framework
from gen_avro import generate_avro_bytes
from datetime import datetime


PROJECT_ID = "qr-code-decideomtour-scanflow"
STORAGE_BUCKET = "snowflake-qrscan-demo"
STORAGE_LOCATION = "qrscan/" # Specify where the avro files will be stored

snow_cli = storage.Client(PROJECT_ID)
snow_bucket = snow_cli.get_bucket(STORAGE_BUCKET)

# Define your Avro schema
AVRO_SCHEMA = avro.schema.parse("""{
  "type": "record",
  "name": "ScanData",
  "fields": [
    {"name": "surname", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "company", "type": "string"},
    {"name": "city", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "scan_time", "type": {"type": "long", "logicalType": "timestamp-nanos"}}
  ]
}""")



def parse_timestamp_nanos(timestamp_nanos):
    # Convert nanoseconds to seconds
    timestamp_seconds = timestamp_nanos // 10**9
    print(timestamp_seconds)
    dt = datetime.fromtimestamp(timestamp_seconds)
    return dt.year, dt.month, dt.day


print(parse_timestamp_nanos(1738053264556000))

# dt = datetime.fromtimestamp(1738053264556000)
# print(dt)
# @functions_framework.cloud_event
# def main(cloud_event):
#     """Handles a Pub/Sub message and uploads Avro data to GCS."""
#     print(cloud_event)

#     pubsub_message = cloud_event.data
#     message_data = pubsub_message.get("message", {}).get("data", None)
#     if not message_data:
#         msg = "no message or data"
#         return msg

#     try:
#         decoded_data = base64.b64decode(message_data).decode("utf-8")
#         data_dict = json.loads(decoded_data)

#         # Uploads Avro bytes to Google Cloud Storage
#         year, month, day = parse_timestamp_nanos(int(data_dict['scan_time']))
#         filename =  f"{data_dict['scan_time']}.avro"
#         storage_filepath = f"{STORAGE_LOCATION}{data_dict['city']}/{year}/{month}/{day}/{filename}"

#         blob = snow_bucket.blob(storage_filepath)

#         avro_bytes = generate_avro_bytes(AVRO_SCHEMA, [data_dict]) # array of 1 row
#         blob.upload_from_string(avro_bytes, content_type="application/avro")

#         print(f"Uploaded  {storage_filepath}")
#         return f"Uploaded  {storage_filepath}"

#     except Exception as e:
#         print(f"Error: {e}")
#         return f"Error: {e}"



# """test"""
# # data_dict = {'surname': 'ALLIAUME', 'name': 'Aurore', 'email': 'aalliaume@nhood.com', 'company': 'Nhood', 'city': 'Lille', 'location': 'atelier_2', 'scan_time': '2025-01-27T14:16:20.798Z'}
# # data_dict = {'surname': 'ALLIAUME2', 'name': 'Aurore2', 'email': 'aalliaume2@nhood.com', 'company': 'Nhood', 'city': 'Lille', 'location': 'atelier_2', 'scan_time': '2025-01-27T14:16:20.728Z'}

# # example_pubsub_message = {'message': {'data': 'eyJzdXJuYW1lIjogIkFMTElBVU1FIiwgIm5hbWUiOiAiQXVyb3JlIiwgImVtYWlsIjogImFhbGxpYXVtZUBuaG9vZC5jb20iLCAiY29tcGFueSI6ICJOaG9vZCIsICJxcl90ZXh0IjogIlwiQUxMSUFVTUVcIjsgXCJBdXJvcmVcIjsgXCJhYWxsaWF1bWVAbmhvb2QuY29tXCI7IFwiTmhvb2RcIiIsICJzY2FuX3RpbWUiOiAiMjAyNS0wMS0yN1QxMDoyNTo1My4yNDBaIn0=', 'messageId': '13650100722526868', 'message_id': '13650100722526868', 'publishTime': '2025-01-27T10:25:55.538Z', 'publish_time': '2025-01-27T10:25:55.538Z'}, 'subscription': 'projects/qr-code-decideomtour-scanflow/subscriptions/eventarc-europe-west9-gcf-scan-write-storage-693050-sub-013'}
# # print(main("example_pubsub_message"))

