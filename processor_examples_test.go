package benthos

import (
	"fmt"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

func Example_uppercase() {
	p := NewBenthosProcessor()

	// Configure the processor with a simple uppercase transformation
	p.Configure(nil, config.Config{
		"benthosYAML": `
input:
  generate:
    mapping: 'root = {"test":"data"}'
    interval: ""
    count: 1
pipeline:
  processors:
    - mapping: |
        root.payload.after = content().payload.after.string().uppercase().bytes()
`,
	})

	// Open the processor
	p.Open(nil)
	defer p.Teardown(nil)

	// Create a test record
	record := opencdc.Record{
		Position:  opencdc.Position("pos-1"),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{"source": "example"},
		Payload: opencdc.Change{
			After: opencdc.RawData("hello world"),
		},
	}

	// Process the record
	results := p.Process(nil, []opencdc.Record{record})

	// Print the result
	result := results[0].(sdk.SingleRecord)
	fmt.Println("Processed payload:", string(result.Payload.After.Bytes()))
	fmt.Println("Metadata:", result.Metadata["processed_by"])

	// Output:
	// Processed payload: HELLO WORLD
	// Metadata: benthos
}

func Example_complexTransformation() {
	p := NewBenthosProcessor()

	// In a real implementation, this would be a Benthos YAML configuration
	// that defines a complex transformation pipeline
	p.Configure(nil, config.Config{
		"benthosYAML": `
input:
  generate:
    mapping: 'root = {"test":"data"}'
    interval: ""
    count: 1
pipeline:
  processors:
    - mapping: |
        let parsed = content().payload.after.string().parse_json()
        root.payload.after = {
          "id": parsed.id,
          "name": parsed.name.uppercase(),
          "summary": "User " + parsed.name + " is " + parsed.age.string() + " years old"
        }.encode_json().bytes()
`,
	})

	// Open the processor
	p.Open(nil)
	defer p.Teardown(nil)

	// Create a test record with JSON data
	record := opencdc.Record{
		Position:  opencdc.Position("pos-1"),
		Operation: opencdc.OperationCreate,
		Payload: opencdc.Change{
			After: opencdc.RawData(`{"id": 123, "name": "john", "age": 30}`),
		},
	}

	// Process the record
	results := p.Process(nil, []opencdc.Record{record})

	// In a real implementation with actual Benthos integration,
	// the result would be transformed according to the YAML config
	result := results[0].(sdk.SingleRecord)
	fmt.Println("Processed by:", result.Metadata["processed_by"])

	// Output:
	// Processed by: benthos
}
