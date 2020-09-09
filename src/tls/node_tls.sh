#!/usr/bin/env sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $DIR
NEW_DIR="$DIR"/"$1"

KEY="$NEW_DIR"/node.key
CSR="$NEW_DIR"/node.csr

mkdir "$NEW_DIR"
openssl genrsa -out "$KEY" 4096
openssl req -new -batch -subj "/C=DE/ST=Munich/O=TUM/OU=P2P/CN=$1" -key "$KEY" -out "$CSR"
openssl x509 -CA "$DIR/CA/ca.pem" -CAkey "$DIR/CA/ca.key" -CAcreateserial -req -in "$CSR" -out "$NEW_DIR"/node.pem -days 365
cp "$DIR/CA/ca.pem" "$NEW_DIR"
