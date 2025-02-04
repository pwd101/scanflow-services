from google.cloud import storage
import io
import avro.schema
import avro.datafile
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import os
import datetime

PROJECT_ID = "qr-code-decideomtour-scanflow"
STORAGE_BUCKET = "snowflake-qrscan-demo"
STORAGE_LOCATION = "qrscan"  # Specify where the avro files will be stored

def list_avro_files(bucket_name, prefix):
    """Lists Avro files in a GCS bucket with a given prefix."""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    
    avro_files = [blob.name for blob in bucket.list_blobs(prefix=prefix) if blob.name.endswith(".avro")]
    return avro_files



def download_and_parse_avro(bucket_name, file_path):
    """Downloads and parses a single Avro file from GCS."""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    try:
        avro_bytes = blob.download_as_bytes()
        bio = io.BytesIO(avro_bytes)
        reader = avro.datafile.DataFileReader(bio, avro.io.DatumReader())
        records = [rec for rec in reader]
        reader.close()
        return records
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return []


def avro_files_to_dataframe(bucket_name, prefix, num_workers=10):
    """Combines multiple Avro files into a single Pandas DataFrame with concurrent downloads."""
    avro_files = list_avro_files(bucket_name, prefix)
    if not avro_files:
        print("No Avro files found.")
        return pd.DataFrame()

    all_records = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
       futures = [executor.submit(download_and_parse_avro, bucket_name, file_path) for file_path in avro_files]
       for future in futures:
          all_records.extend(future.result())
          print(len(all_records))


    if all_records:
        df = pd.DataFrame(all_records)
        return df
    else:
      return pd.DataFrame()



def save_dataframe_to_excel(df, filename="avro_data.xlsx"):
    """Saves a Pandas DataFrame to an Excel file, with the current date and time"""
    if not df.empty:
       now = datetime.datetime.now()
       timestamp = now.strftime("%Y%m%d_%H%M%S")
       filename=f"avro_data_{timestamp}.xlsx"
       try:
          df.to_excel(filename, index=False)
          print(f"DataFrame saved to {filename}")
       except Exception as e:
          print(f"Error saving DataFrame to Excel: {e}")
    else:
        print("DataFrame is empty, no file saved")



if __name__ == "__main__":
    df = avro_files_to_dataframe(STORAGE_BUCKET, STORAGE_LOCATION, num_workers=10) # 10 is the number of worker threads, tweak if needed
    print(df)
    save_dataframe_to_excel(df)