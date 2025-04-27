#!/bin/bash

# Set up the test environment
cd "$(dirname "$0")"
mkdir -p test
cd test

# Create an input file
echo "hello world" > example.in
echo "this is a test" >> example.in
echo "benthos processor" >> example.in

# Build and run the processor
cd ../../
go build
./conduit-processor-benthos &
PROCESSOR_PID=$!

# Wait for the processor to start
sleep 2

# Create the pipeline using the Conduit CLI
conduit pipeline create -f examples/benthos-processor-example.yaml

# Wait for processing to complete
sleep 5

# Check the results
echo "Input file:"
cat examples/test/example.in
echo -e "\nOutput file (raw):"
cat examples/test/example.out
echo -e "\nOutput file (decoded):"
cat examples/test/example.out | jq '.payload.after |= @base64d'

# Clean up
conduit pipeline delete pipeline-with-benthos-processor
kill $PROCESSOR_PID
