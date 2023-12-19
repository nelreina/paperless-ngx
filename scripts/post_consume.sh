#!/usr/bin/env bash

echo "

A document with an id of ${DOCUMENT_ID} was just consumed.  I know the
following additional information about it:

* Generated File Name: ${DOCUMENT_FILE_NAME}
* Created: ${DOCUMENT_CREATED}
* Tags: ${DOCUMENT_TAGS}


"

python /usr/src/paperless/src/scripts/add_to_stream.py