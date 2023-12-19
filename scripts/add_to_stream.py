import logging
import sys
import uuid
from os import environ
from redis import Redis
from datetime import datetime
import json


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(message)s')
stream_key = environ.get("STREAM_KEY", "paperless-server:stream")
service_name = environ.get("SERVICE_NAME", "paperless-server")


def connect_to_redis():
    hostname = environ.get("REDIS_HOST", "localhost")
    port = environ.get("REDIS_PORT", 6379)
    password = environ.get("REDIS_PW")

    r = Redis(hostname, port, password=password,
              retry_on_timeout=True, client_name=service_name)
    return r

def addToStream(redis, event,  aggregateId, payload):
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        stream_data = {
            b"event": event,
            b"aggregateId": aggregateId,
            b"timestamp": timestamp,
            b"payload": payload
        }
        redis.xadd(stream_key, stream_data)
        pass



DOCUMENT_TAGS = environ.get("DOCUMENT_TAGS")
logging.debug('add_to_stream.py --- DOCUMENT_TAGS: %s', DOCUMENT_TAGS)

DOCUMENT_ID = environ.get("DOCUMENT_ID")
logging.debug('add_to_stream.py --- DOCUMENT_ID: %s', DOCUMENT_ID)

DOCUMENT_FILE_NAME = environ.get("DOCUMENT_FILE_NAME")


documentId = DOCUMENT_ID
filename = DOCUMENT_FILE_NAME
tags = DOCUMENT_TAGS

# generate payload
payload = {
    "documentId": documentId,
    "filename": filename,
    "tags": tags
}
# create json string from payload
json = json.dumps(payload)
logging.debug('Payload: %s', json)

# generate uuid
aggregateId = uuid.uuid4()
logging.debug('Aggregate ID: %s', aggregateId)

logging.debug('Adding Document ID: %s', documentId)
redis = connect_to_redis()
addToStream(redis, "DocumentAdded", str(aggregateId), json)
logging.debug('Added Document ID: %s', documentId)
redis.close()
