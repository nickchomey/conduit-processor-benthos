# Benthos Processor Examples

This directory contains examples of how to use the Benthos processor in Conduit pipelines.

## Basic Example

The `benthos-processor-example.yaml` file demonstrates a simple pipeline that:
- Reads data from a file
- Processes it through Benthos (converts text to uppercase)
- Writes the results to another file

To test this example, run:

```bash
./test-benthos-processor.sh
```

## Advanced Example

The `benthos-advanced-example.yaml` file demonstrates a more complex pipeline that:
- Reads JSON data from a file
- Processes it through Benthos with multiple transformations:
  - Parses the JSON
  - Transforms the data (uppercase names, adds a summary field)
  - Adds metadata
- Writes the results to another file

To test this example, run:

```bash
./test-benthos-advanced.sh
```

## Creating Your Own Pipelines

To create your own pipeline using the Benthos processor:

1. Create a YAML file with your pipeline configuration
2. Include a processor with the plugin type "benthos"
3. Configure the `benthosYAML` setting with your Benthos pipeline configuration
4. Create the pipeline using the Conduit CLI: `conduit pipeline create -f your-pipeline.yaml`

Example processor configuration:

```yaml
processors:
  - id: benthos-processor
    plugin: "benthos"
    settings:
      benthosYAML: |
        pipeline:
          processors:
            - mapping: |
                # Your Benthos mapping here
```

For more information on Benthos mapping language, see the [Benthos documentation](https://www.benthos.dev/docs/guides/bloblang/about).
