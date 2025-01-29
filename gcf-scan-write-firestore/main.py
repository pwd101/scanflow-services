import base64
import json
import functions_framework
from google.cloud import firestore
from datetime import datetime

PROJECT_ID = "qr-code-decideomtour-scanflow"
FIRESTORE_COLL = "scanflow_scans"

firestore_db = firestore.Client(PROJECT_ID)


@functions_framework.cloud_event
def main(cloud_event):
    
    pubsub_message = cloud_event.data
    message_data = pubsub_message.get("message", {}).get("data", None)
    if not message_data:
        msg = "no message or data"
        return msg

    try:
        decoded_data = base64.b64decode(message_data).decode("utf-8")
        data_dict = json.loads(decoded_data)
        data_dict = data_dict = {'scan_id': "test", 'surname': 'ALLIAUME2', 'name': 'Aurore2', 'email': 'aalliaume2@nhood.com', 'company': 'Nhood', 'city': 'Lille', 'location': 'atelier_2', 'scan_time': '2025-01-27T14:16:20.728Z'}
        data_dict["scan_time"] = datetime.fromisoformat(data_dict["scan_time"].replace('Z', '+00:00'))

        doc_ref = firestore_db.document(FIRESTORE_COLL, data_dict['scan_id'])
        doc_ref.set(data_dict)

        return f"Document {data_dict['scan_id']} added"

    except Exception as e:
        print(f"Error: {e}")
        return f"Error: {e}"


# main("")