#!/bin/bash

# Compilando um arquivo Protocol Buffer manualmeente usando o protoc
protoc -I=proto \
    --go_opt=paths=source_relative \
    --go_out=./proto/gen \
    --go-grpc_opt=paths=source_relative \
    --go-grpc_out=./proto/gen \
    ./proto/*.proto
