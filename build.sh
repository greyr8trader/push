#!/usr/bin/env bash
# Generate gRPC Python stubs from the .proto file.
# Run on every Railway build so stubs stay in sync with the schema.
set -euo pipefail

python -m grpc_tools.protoc \
  -I=./proto \
  --python_out=. \
  --grpc_python_out=. \
  proto/broker_api.proto

echo "Generated broker_api_pb2.py and broker_api_pb2_grpc.py"
