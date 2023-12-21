import logging
import sys
import uuid
from os import environ
from redis import Redis
from datetime import datetime
import json
import requests


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(message)s')
stream_key = environ.get("STREAM_KEY", "paperless-server:stream")
service_name = environ.get("SERVICE_NAME", "paperless-server")


PAPERLESS_SECRET_KEY = environ.get("PAPERLESS_SECRET_KEY")
PAPERLESS_URL = environ.get("PAPERLESS_URL", "http://localhost:8000")
headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization":"Token " + PAPERLESS_SECRET_KEY}

def get_document(documentId):
    # logging.debug('PAPERLESS_SECRET_KEY: %s', PAPERLESS_SECRET_KEY)
    logging.debug('PAPERLESS_URL: %s', PAPERLESS_URL)
    url = f"{PAPERLESS_URL}/api/documents/{documentId}/?format=json"
    response = requests.get(url, headers=headers)

    return response.json()

def get_document_type(id):
    url = f"{PAPERLESS_URL}/api/document_types/{id}/?format=json"
    response = requests.get(url, headers=headers)
    return response.json()

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
DOCUMENT_ORIGINAL_FILENAME = environ.get("DOCUMENT_ORIGINAL_FILENAME")


documentId = DOCUMENT_ID
filename = DOCUMENT_FILE_NAME
original_filename = DOCUMENT_ORIGINAL_FILENAME
tags = DOCUMENT_TAGS

document = get_document(documentId)
document_type_id = document.get("document_type")
# If document_type_id is None, then the document is a receipt
if document_type_id is None:
    document_type = ""
else: 
    document_type = get_document_type(document_type_id)

# generate payload
payload = {
    "documentId": documentId,
    "filename": filename,
    "original_filename": original_filename,
    "tags": tags,
    "content": document.get("content"),
    "type": document_type.get("name"),
}
# create json string from payload
json = json.dumps(payload)
logging.debug('Payload: %s', json)

aggregateId =  original_filename.split(".")[0]

logging.debug('Adding Document ID: %s', documentId)
redis = connect_to_redis()
addToStream(redis, "DocumentAdded", aggregateId, json)
logging.debug('Added Document ID: %s', documentId)
redis.close()
