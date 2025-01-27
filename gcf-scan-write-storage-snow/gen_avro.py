import io
import snappy
import avro.schema
from avro.datafile import  DataFileWriter
from avro.io import DatumWriter


def validate_data_dict(schema:avro.schema, data_dict: dict):
    expected_keys = [field.name for field in schema.fields]

    if not all(key in data_dict for key in expected_keys):
        raise ValueError("Dictionary is missing required fields. Should contain: " + ", ".join(expected_keys))
    for key in expected_keys:
        if not isinstance(data_dict[key], str):
            raise ValueError(f"Value of '{key}' should be a string, but it is a: {type(data_dict[key])}")
        


#Generate avro bytes as a in-memory file, with the schema.
def generate_avro_bytes(schema,avro_dicts, codec='snappy'):
    bytes_writer = io.BytesIO()
    writer = DataFileWriter(bytes_writer, DatumWriter(), schema, codec=codec)
    for d in avro_dicts:
        writer.append(d)
    writer.flush()
    bytes_value = bytes_writer.getvalue()
    writer.close()
    return bytes_value



# if __name__ == '__main__':

#     AVRO_SCHEMA = avro.schema.parse("""{
#     "type": "record",
#     "name": "ScanData",
#     "fields": [
#         {"name": "surname", "type": "string"},
#         {"name": "name", "type": "string"},
#         {"name": "email", "type": "string"},
#         {"name": "company", "type": "string"},
#         {"name": "city", "type": "string"},
#         {"name": "location", "type": "string"},
#         {"name": "scan_time", "type": "string"}
#     ]
#     }""")


#     data = [{
#         "surname": "Doe",
#         "name": "John",
#         "email": "john.doe@example.com",
#         "company": "Acme Corp",
#         "city": "New York",
#         "location": "Conference Hall",
#         "scan_time": "2023-10-27 10:00:00"
#     }]



#     data = generate_avro_bytes(AVRO_SCHEMA, data )
#     print(data)



