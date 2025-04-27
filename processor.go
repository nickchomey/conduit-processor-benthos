package benthos

import (
	"context"
	"fmt"
	"sync"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/warpstreamlabs/bento/public/service"
)

//go:generate paramgen -output=paramgen_proc.go ProcessorConfig

type Processor struct {
	sdk.UnimplementedProcessor

	config ProcessorConfig

	// channels for communication with Benthos
	records chan opencdc.Record
	results chan processResult
	errC    chan error

	// mutex to protect concurrent access
	mu sync.Mutex

	// Benthos components
	benthosStream *service.Stream
	cancelBenthos context.CancelFunc
}

type processResult struct {
	record opencdc.Record
	err    error
}

type ProcessorConfig struct {
	// BenthosYAML is the YAML configuration for the Benthos pipeline
	BenthosYAML string `json:"benthosYAML" validate:"required"`
}

func NewProcessor() sdk.Processor {
	// Create Processor. The default middleware will be automatically added
	// by the SDK when the processor is run.
	return &Processor{
		records: make(chan opencdc.Record),
		results: make(chan processResult),
		errC:    make(chan error, 1),
	}
}

func (p *Processor) Specification() (sdk.Specification, error) {
	return sdk.Specification{
		Name:        "benthos",
		Summary:     "Process records through a Benthos pipeline",
		Description: "A processor that passes Conduit records through a Benthos pipeline for advanced processing",
		Version:     "v0.1.0",
		Author:      "Conduit",
		Parameters:  ProcessorConfig{}.Parameters(),
	}, nil
}

func (p *Processor) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Debug().Msg("Configuring Benthos processor...")

	err := sdk.ParseConfig(ctx, cfg, &p.config, ProcessorConfig{}.Parameters())
	if err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}

	if p.config.BenthosYAML == "" {
		return fmt.Errorf("benthosYAML configuration is required")
	}

	return nil
}

func (p *Processor) Open(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msg("Opening Benthos processor...")

	// Create a new Benthos stream builder
	builder := service.NewStreamBuilder()
	builder.DisableLinting()

	// Register our custom input and output for Benthos
	err := service.RegisterInput(
		"conduit_processor_input",
		service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return p, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed registering Benthos input: %w", err)
	}

	err = service.RegisterOutput(
		"conduit_processor_output",
		service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			return p, 1, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed registering Benthos output: %w", err)
	}

	// Set the Benthos YAML configuration
	err = builder.SetYAML(p.config.BenthosYAML)
	if err != nil {
		return fmt.Errorf("failed parsing Benthos YAML config: %w", err)
	}

	// Add our custom input and output to the Benthos pipeline
	builder.AddInputYAML(`
label: "conduit_processor_input"
conduit_processor_input: {}
`)

	builder.AddOutputYAML(`
label: "conduit_processor_output"
conduit_processor_output: {}
`)

	// Build the Benthos stream
	stream, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed building Benthos stream: %w", err)
	}
	p.benthosStream = stream

	// Run the Benthos stream in a goroutine
	benthosCtx, cancelBenthos := context.WithCancel(context.Background())
	p.cancelBenthos = cancelBenthos

	go func() {
		sdk.Logger(ctx).Debug().Msg("Running Benthos stream...")
		err = stream.Run(benthosCtx)
		if err != nil {
			sdk.Logger(ctx).Err(err).Msg("Benthos stream error")
			p.errC <- err
		}
	}()

	return nil
}

func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	sdk.Logger(ctx).Debug().Int("count", len(records)).Msg("Processing records through Benthos")

	out := make([]sdk.ProcessedRecord, 0, len(records))

	for _, record := range records {
		// Process each record through Benthos
		processedRecord, err := p.processRecord(ctx, record)
		if err != nil {
			return append(out, sdk.ErrorRecord{Error: err})
		}

		out = append(out, sdk.SingleRecord(processedRecord))
	}

	return out
}

func (p *Processor) processRecord(ctx context.Context, record opencdc.Record) (opencdc.Record, error) {
	// Send the record to Benthos for processing
	select {
	case p.records <- record:
		// Record sent to Benthos
	case err := <-p.errC:
		return opencdc.Record{}, fmt.Errorf("Benthos stream error: %w", err)
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	}

	// Wait for the processed record
	select {
	case result := <-p.results:
		if result.err != nil {
			return opencdc.Record{}, result.err
		}
		return result.record, nil
	case err := <-p.errC:
		return opencdc.Record{}, fmt.Errorf("Benthos stream error: %w", err)
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	}
}

func (p *Processor) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msg("Tearing down Benthos processor...")

	if p.cancelBenthos != nil {
		p.cancelBenthos()
	}

	return nil
}

// Implement service.Input interface for Benthos

func (p *Processor) Connect(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msg("Benthos input connect")
	return nil
}

func (p *Processor) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	sdk.Logger(ctx).Debug().Msg("Benthos input read")

	// Wait for a record to process
	select {
	case record := <-p.records:
		// Convert Conduit record to Benthos message
		msg := p.toMessage(record)

		return msg, func(ctx context.Context, err error) error {
			if err != nil {
				p.results <- processResult{err: err}
			}
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (p *Processor) Close(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msg("Benthos input close")
	return nil
}

// Implement service.Output interface for Benthos

func (p *Processor) Write(ctx context.Context, msg *service.Message) error {
	sdk.Logger(ctx).Debug().Msg("Benthos output write")

	// Convert Benthos message back to Conduit record
	record, err := p.fromMessage(msg)
	if err != nil {
		return err
	}

	// Send the processed record back
	p.results <- processResult{record: record}

	return nil
}

// Helper methods for converting between Conduit records and Benthos messages

func (p *Processor) toMessage(record opencdc.Record) *service.Message {
	msg := service.NewMessage(nil)

	// Convert record to structured data for Benthos
	data := map[string]interface{}{
		"position":  record.Position,
		"operation": record.Operation.String(),
		"metadata":  record.Metadata,
	}

	// Handle Key
	if record.Key != nil {
		data["key"] = record.Key.Bytes()
	}

	// Handle Payload
	payload := map[string]interface{}{}
	if record.Payload.Before != nil {
		payload["before"] = record.Payload.Before.Bytes()
	}
	if record.Payload.After != nil {
		payload["after"] = record.Payload.After.Bytes()
	}
	data["payload"] = payload

	msg.SetStructured(data)
	return msg
}

func (p *Processor) fromMessage(msg *service.Message) (opencdc.Record, error) {
	// Get the structured data from the message
	structured, err := msg.AsStructured()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to get structured data from Benthos message: %w", err)
	}

	data, ok := structured.(map[string]interface{})
	if !ok {
		return opencdc.Record{}, fmt.Errorf("Benthos message is not a map")
	}

	// Create a new record
	record := opencdc.Record{}

	// Set Position
	if pos, ok := data["position"]; ok {
		if posStr, ok := pos.(string); ok {
			record.Position = opencdc.Position(posStr)
		}
	}

	// Set Operation
	if op, ok := data["operation"]; ok {
		if opStr, ok := op.(string); ok {
			// Convert string to Operation
			switch opStr {
			case "create":
				record.Operation = opencdc.OperationCreate
			case "update":
				record.Operation = opencdc.OperationUpdate
			case "delete":
				record.Operation = opencdc.OperationDelete
			case "snapshot":
				record.Operation = opencdc.OperationSnapshot
			default:
				record.Operation = opencdc.OperationCreate
			}
		} else {
			// Default to create if not specified
			record.Operation = opencdc.OperationCreate
		}
	}

	// Set Metadata
	if meta, ok := data["metadata"]; ok {
		if metaMap, ok := meta.(map[string]interface{}); ok {
			record.Metadata = make(opencdc.Metadata)
			for k, v := range metaMap {
				if vStr, ok := v.(string); ok {
					record.Metadata[k] = vStr
				}
			}
		}
	}

	// Set Key
	if key, ok := data["key"]; ok {
		if keyBytes, ok := key.([]byte); ok {
			record.Key = opencdc.RawData(keyBytes)
		}
	}

	// Set Payload
	if payload, ok := data["payload"]; ok {
		if payloadMap, ok := payload.(map[string]interface{}); ok {
			// Set Before
			if before, ok := payloadMap["before"]; ok {
				if beforeBytes, ok := before.([]byte); ok {
					record.Payload.Before = opencdc.RawData(beforeBytes)
				}
			}

			// Set After
			if after, ok := payloadMap["after"]; ok {
				if afterBytes, ok := after.([]byte); ok {
					record.Payload.After = opencdc.RawData(afterBytes)
				}
			}
		}
	}

	return record, nil
}
