import pandas as pd
import datetime

df = pd.read_excel("./avro_data_20250203_180413.xlsx")

df["location"] = df["location"].str.replace("_", " ")
df["location"] = df["location"].str.replace("Paris.", "")
df["location"] = df["location"].str.replace("Lille.", "")


def datetime_str_to_nanosecond_timestamp(datetime_str):
    """Converts an ISO datetime string with 'Z' timezone to a nanosecond timestamp."""
    try:
        # Parse the datetime string using fromisoformat, replacing Z with +00:00
        dt_obj = datetime.datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
        
        # Get the timestamp as a float (seconds with fractions) using timestamp method
        timestamp_seconds = dt_obj.timestamp()
        
        # Convert to nanoseconds
        timestamp_nanoseconds = int(timestamp_seconds * 1_000_000)
        return timestamp_nanoseconds

    except ValueError:
         print(f"Error parsing datetime: Invalid format, expected ISO string ending in Z")
         return None

df["scan_time"] = df["scan_time"].apply(datetime_str_to_nanosecond_timestamp)
df = df.drop(columns=['scan_id'])

df.to_excel("avro_data_clean.xlsx", index=False)
print(df)
