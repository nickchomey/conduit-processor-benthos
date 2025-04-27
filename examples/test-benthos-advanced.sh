#!/bin/bash

# Set up the test environment
cd "$(dirname "$0")"
mkdir -p test
cd test

# Create an input file with JSON data
cat > json-example.in << EOF
{"id": 1, "name": "john", "age": 30}
{"id": 2, "name": "alice", "age": 25}
{"id": 3, "name": "bob", "age": 35}
EOF

# Build and run the processor
cd ../../
go build
./conduit-processor-benthos &
PROCESSOR_PID=$!

# Wait for the processor to start
sleep 2

# Create the pipeline using the Conduit CLI
conduit pipeline create -f examples/benthos-advanced-example.yaml

# Wait for processing to complete
sleep 5

# Check the results
echo "Input file:"
cat examples/test/json-example.in
echo -e "\nOutput file (raw):"
cat examples/test/json-example.out
echo -e "\nOutput file (decoded):"
cat examples/test/json-example.out | jq '.payload.after |= @base64d | .payload.after |= fromjson'

# Clean up
conduit pipeline delete benthos-advanced-pipeline
kill $PROCESSOR_PID
