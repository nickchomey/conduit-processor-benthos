# Conduit Processor for Benthos

[Conduit](https://conduit.io) processor that integrates with [Benthos](https://www.benthos.dev/) (via the [Bento](https://github.com/warpstreamlabs/bento) fork) to leverage its rich processing capabilities for transforming Conduit records.

## How to build?

Run `make build` to build the processor.

## Testing

Run `make test` to run all the unit tests.

## Functionality

This processor allows you to pass Conduit OpenCDC records to an embedded Benthos server/process, which will then process the records using Benthos's comprehensive set of processors, and return the processed records back to Conduit.

Benthos offers a wide range of processors for data transformation, filtering, enrichment, and more, making it a powerful tool for complex data processing pipelines.

### Processor Configuration

| name          | description                              | required | default value |
|---------------|------------------------------------------|----------|---------------|
| `benthosYAML` | YAML configuration for the Benthos pipeline | true     | ""            |

### Example Configuration

```yaml
processors:
  - id: benthos-processor
    type: benthos
    config:
      benthosYAML: |
        pipeline:
          processors:
            - mapping: |
                root.payload.after = content().payload.after.string().uppercase().bytes()
```

### Example Usage

#### Simple Transformation

```go
// Create and configure the processor
p := benthos.NewProcessor()
p.Configure(ctx, config.Config{
    "benthosYAML": `
pipeline:
  processors:
    - mapping: |
        root.payload.after = content().payload.after.string().uppercase().bytes()
`,
})

// Open the processor
p.Open(ctx)
defer p.Teardown(ctx)

// Process records
results := p.Process(ctx, records)
```

#### JSON Transformation

```go
// Create and configure the processor
p := benthos.NewProcessor()
p.Configure(ctx, config.Config{
    "benthosYAML": `
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
p.Open(ctx)
defer p.Teardown(ctx)

// Process records
results := p.Process(ctx, records)
```

## Known Issues & Limitations

- This is currently a simplified implementation that simulates Benthos processing
- Full integration with Benthos/Bento requires additional work
- Error handling and retry mechanisms need improvement

## Planned work

- [ ] Full integration with Benthos/Bento
- [ ] Support for all Benthos processors and transformations
- [ ] Improved error handling and retry mechanisms
- [ ] Performance optimizations
