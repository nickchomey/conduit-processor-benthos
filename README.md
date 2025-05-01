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

| name            | description                                                | required | default value |
|-----------------|-----------------------------------------------------------|----------|---------------|
| `benthosYAML`   | YAML configuration for the Benthos processors section. This defines the Benthos processors that will be applied to each record. You can use any Benthos processor documented at [benthos.dev](https://benthos.dev/docs/components/processors/about/). | true     | ""            |
| `bufferSize`    | Size of internal channels for processing records. Higher values can improve throughput but use more memory. This controls how many records can be buffered internally while waiting for processing. | false    | 10            |
| `threadCount`   | Number of parallel processing threads in the Benthos pipeline. Higher values can improve throughput for CPU-bound processors. For IO-bound processors, increasing this may not improve performance. | false    | 1             |
| `logLevel`      | Controls the verbosity of Benthos internal logs. Valid values: NONE, ERROR, WARN, INFO, DEBUG, TRACE. | false    | "INFO"        |

### Example Configurations

#### Basic Text Transformation

```yaml
processors:
  - id: benthos-processor
    type: benthos
    config:
      # Required: Benthos pipeline configuration in YAML format
      benthosYAML: |
        pipeline:
          processors:
            - mapping: |
                root.payload.after = content().payload.after.string().uppercase().bytes()

      # Optional performance tuning parameters
      bufferSize: 20          # Increase for higher throughput (default: 10)
      threadCount: 4          # Use multiple threads for parallel processing (default: 1)
      logLevel: "INFO"        # Control Benthos log verbosity (default: "INFO")
```

#### JSON Transformation

```yaml
processors:
  - id: json-transform
    type: benthos
    config:
      benthosYAML: |
        pipeline:
          processors:
            - bloblang: |
                # Parse JSON from the payload
                let data = this.payload.after.string().parse_json()

                # Transform and create new JSON structure
                root.payload.after = {
                  "id": data.id,
                  "user": {
                    "name": data.name.uppercase(),
                    "email": data.email.lowercase(),
                    "role": if data.admin { "ADMIN" } else { "USER" }
                  },
                  "metadata": {
                    "processed_at": now().format_timestamp(),
                    "source": meta("source") ?? "unknown"
                  }
                }.encode_json().bytes()
```

#### HTTP Enrichment

```yaml
processors:
  - id: http-enrichment
    type: benthos
    config:
      benthosYAML: |
        pipeline:
          processors:
            # Extract user ID from the record
            - bloblang: |
                let user_id = this.payload.after.string().parse_json().user_id
                meta user_id = user_id

            # Make HTTP request to external API
            - http:
                url: https://api.example.com/users/${! meta("user_id") }
                verb: GET
                headers:
                  Authorization: Bearer ${! env("API_TOKEN") }
                rate_limit: 10
                timeout: 5s

            # Process the response and merge with original data
            - bloblang: |
                let original = this.payload.after.string().parse_json()
                let user_data = meta("http_response").string().parse_json()

                root.payload.after = {
                  "id": original.id,
                  "transaction": original.transaction,
                  "user": user_data,
                  "enriched": true
                }.encode_json().bytes()
```

#### Conditional Processing

```yaml
processors:
  - id: conditional-processor
    type: benthos
    config:
      benthosYAML: |
        pipeline:
          processors:
            - bloblang: |
                let data = this.payload.after.string().parse_json()

                # Add metadata for routing
                meta transaction_type = data.type
                meta amount = data.amount

                # Keep the original data
                root = this

            # Apply different processing based on conditions
            - switch:
                cases:
                  - check: meta("transaction_type") == "purchase" && meta("amount").number() > 1000
                    processors:
                      - bloblang: |
                          let data = this.payload.after.string().parse_json()
                          root.payload.after = {
                            "id": data.id,
                            "type": "high_value_purchase",
                            "amount": data.amount,
                            "needs_review": true
                          }.encode_json().bytes()

                  - check: meta("transaction_type") == "refund"
                    processors:
                      - bloblang: |
                          let data = this.payload.after.string().parse_json()
                          root.payload.after = {
                            "id": data.id,
                            "type": "refund_processed",
                            "amount": data.amount,
                            "processed_at": now()
                          }.encode_json().bytes()
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

## Features

- **Full Benthos Integration**: Uses the Bento library to provide access to all Benthos processors
- **Memory Efficient**: Uses object pools to reduce GC pressure
- **Conduit Batching**: Works seamlessly with Conduit's native batch processing
- **Parallel Processing**: Configurable thread count for CPU-bound workloads
- **Hot Reload**: Update processor configuration without restarting
- **Error Handling**: Comprehensive error handling with detailed logging

## Benthos Bloblang

[Bloblang](https://benthos.dev/docs/guides/bloblang/about/) is Benthos's powerful data mapping language, designed specifically for data transformation. It's a key feature that makes this processor so versatile.

### Key Bloblang Features

- **Type-safe**: Bloblang is strongly typed, helping prevent runtime errors
- **Declarative**: Focus on what you want to transform, not how
- **Composable**: Build complex transformations from simple functions
- **Powerful**: Rich set of functions for string, numeric, and collection operations

### Common Bloblang Patterns

#### Accessing Record Data

```bloblang
# Access the payload.after field from a Conduit record
let data = this.payload.after.string().parse_json()

# Access specific fields
let user_id = data.user_id
let timestamp = data.timestamp
```

#### Transforming Data

```bloblang
# Create a new structure
root.payload.after = {
  "id": data.id,
  "user": {
    "name": data.name.uppercase(),
    "email": data.email.lowercase()
  },
  "processed": true,
  "timestamp": now()
}.encode_json().bytes()
```

#### Conditional Logic

```bloblang
# If-else statements
let status = if data.amount > 1000 {
  "high_value"
} else if data.amount > 500 {
  "medium_value"
} else {
  "low_value"
}

# Ternary-style conditionals
let is_admin = data.role == "admin" ? true : false
```

#### Working with Metadata

```bloblang
# Set metadata
meta source = "benthos_processor"
meta timestamp = now().format_timestamp()

# Get metadata
let source = meta("source")
```

For more details on Bloblang, see the [official documentation](https://benthos.dev/docs/guides/bloblang/about/).

## Advanced Usage

### Hot Reloading Configuration

The processor supports updating the Benthos configuration at runtime:

```go
// Update the processor configuration without restarting
err := processor.UpdateBenthosStream(ctx, newBenthosYAML)
if err != nil {
    log.Fatalf("Failed to update configuration: %v", err)
}
```

## Known Issues & Limitations

- Some advanced Benthos features may require additional configuration
- Very large messages may cause memory pressure despite pooling

## Future Enhancements

- [ ] Support for Benthos plugins
- [ ] Dynamic scaling of processing threads
- [ ] Circuit breaker pattern for error handling
- [ ] Improved schema handling and validation
