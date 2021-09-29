import json 


def json_serializer(msg):
    return json.dumps(msg).encode('utf-8')


def on_send_success(record_metadata):
    print("Request succeed on Topic #{} in Partition #{} with Offset #{}".
          format(record_metadata.topic, record_metadata.partition, record_metadata.offset))


def on_send_error(excp):
    print("An error has occurred: ", excp)
