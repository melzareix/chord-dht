#!/usr/bin/env bash

# sign the passed csr
openssl x509 -CA ca.pem -CAkey ca.key -CAcreateserial -req -in "$1" -out node.pem -days 365
