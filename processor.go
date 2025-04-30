package benthos

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit/pkg/foundation/log"
	"github.com/google/uuid" // Import UUID package
	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
)

//go:generate paramgen -output=paramgen_proc.go ProcessorConfig

// --- Global Registry ---

// processorRegistry holds active BenthosProcessor instances keyed by a unique ID.
var (
	processorRegistry = make(map[string]*BenthosProcessor)
	registryMutex     sync.RWMutex // Mutex to protect concurrent access to the registry
)

// baseBenthosConfigYAML defines the static input/output structure for the Benthos stream.
// The INSTANCE_ID placeholder will be replaced with the processor's unique ID.
const baseBenthosConfigYAML = `
input:
  conduit_processor_input:
    instance_id: ${INSTANCE_ID}
output:
  conduit_processor_output:
    instance_id: ${INSTANCE_ID}
# pipeline: processors will be added dynamically
`

// --- Benthos Component Wrappers and Registration ---

// conduitInputConfig is the config structure for our custom Benthos input.
type conduitInputConfig struct {
	InstanceID string `json:"instance_id"`
}

// conduitOutputConfig is the config structure for our custom Benthos output.
type conduitOutputConfig struct {
	InstanceID string `json:"instance_id"`
}

// conduitInputWrapper implements service.Input, reading from a specific processor's channel.
type conduitInputWrapper struct {
	p      *BenthosProcessor
	logger log.CtxLogger
}

// conduitOutputWrapper implements service.Output, writing to a specific processor's channel.
type conduitOutputWrapper struct {
	p      *BenthosProcessor
	logger log.CtxLogger
}

// Ensure Benthos input/output plugins are registered only once globally
func init() {
	inputConfSpec := service.NewConfigSpec().
		Field(service.NewStringField("instance_id").Description("The unique ID of the processor instance."))

	err := service.RegisterInput(
		"conduit_processor_input",
		inputConfSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			// Fix: Use FieldString instead of Unmarshal
			instanceID, err := conf.FieldString("instance_id")
			if err != nil {
				return nil, fmt.Errorf("failed to get instance_id for conduit_processor_input: %w", err)
			}
			if instanceID == "" {
				return nil, fmt.Errorf("instance_id is required for conduit_processor_input")
			}

			registryMutex.RLock()
			p, ok := processorRegistry[instanceID]
			registryMutex.RUnlock()

			if !ok {
				return nil, fmt.Errorf("processor instance %q not found in registry", instanceID)
			}

			// Wrap with AutoRetryNacks for automatic retry of failed messages
			return service.AutoRetryNacks(&conduitInputWrapper{p: p, logger: p.logger.WithComponent("benthos.input")}), nil
		},
	)
	if err != nil {
		panic(fmt.Sprintf("failed registering Benthos input 'conduit_processor_input': %v", err))
	}

	outputConfSpec := service.NewConfigSpec().
		Field(service.NewStringField("instance_id").Description("The unique ID of the processor instance."))

	err = service.RegisterOutput(
		"conduit_processor_output",
		outputConfSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			// Fix: Use FieldString instead of Unmarshal
			instanceID, err := conf.FieldString("instance_id")
			if err != nil {
				return nil, 0, fmt.Errorf("failed to get instance_id for conduit_processor_output: %w", err)
			}
			if instanceID == "" {
				return nil, 0, fmt.Errorf("instance_id is required for conduit_processor_output")
			}

			registryMutex.RLock()
			p, ok := processorRegistry[instanceID]
			registryMutex.RUnlock()

			if !ok {
				return nil, 0, fmt.Errorf("processor instance %q not found in registry", instanceID)
			}

			return &conduitOutputWrapper{p: p, logger: p.logger.WithComponent("benthos.output")}, 1, nil
		},
	)
	if err != nil {
		panic(fmt.Sprintf("failed registering Benthos output 'conduit_processor_output': %v", err))
	}
}

// --- BenthosProcessor Struct ---

type BenthosProcessor struct {
	sdk.UnimplementedProcessor

	config ProcessorConfig

	// channels for communication with Benthos
	records chan opencdc.Record
	results chan processResult
	errC    chan error // For receiving fatal errors from the Benthos stream goroutine

	// mutex to protect concurrent access during stream updates and processing
	// Use RWMutex: multiple readers (Process) can run concurrently,
	// but updates (updateBenthosStream, Teardown) need exclusive write lock.
	mu sync.RWMutex

	// Benthos components
	benthosStream *service.Stream
	cancelBenthos context.CancelFunc

	// Unique ID for this processor instance
	instanceID string

	// Logger instance
	logger log.CtxLogger
}

type processResult struct {
	record opencdc.Record
	err    error
}

type ProcessorConfig struct {
	// BenthosYAML is the YAML configuration for the Benthos *processors* section
	BenthosYAML string `json:"benthosYAML" validate:"required"`
}

// NewBenthosProcessor creates a new Benthos processor with the provided logger.
func NewBenthosProcessor(logger log.CtxLogger) *BenthosProcessor {
	return &BenthosProcessor{
		records: make(chan opencdc.Record), // Unbuffered might be okay if Benthos reads quickly
		results: make(chan processResult),  // Unbuffered might be okay
		errC:    make(chan error, 1),       // Buffered to avoid blocking the stream goroutine on error
		logger:  logger.WithComponent("processor.benthos"),
	}
}

func (p *BenthosProcessor) Specification() (sdk.Specification, error) {
	return sdk.Specification{
		Name:        "benthos",
		Summary:     "Process records through a Benthos pipeline",
		Description: "A processor that passes Conduit records through a Benthos pipeline for advanced processing",
		Version:     "v0.1.0",
		Author:      "Conduit",
		Parameters:  ProcessorConfig{}.Parameters(),
	}, nil
}

func (p *BenthosProcessor) Configure(ctx context.Context, cfg config.Config) error {
	p.logger.Debug(ctx).Msg("Configuring Benthos processor...")
	// Parse and store the processor-specific YAML provided by the user/Conduit config
	err := sdk.ParseConfig(ctx, cfg, &p.config, ProcessorConfig{}.Parameters())
	if err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}
	p.logger.Debug(ctx).Str("processorYAML", p.config.BenthosYAML).Msg("Benthos processor YAML configured")
	return nil
}

func (p *BenthosProcessor) Open(ctx context.Context) error {
	p.logger.Debug(ctx).Msg("Opening Benthos processor...")

	// Generate unique ID for this instance
	p.instanceID = uuid.NewString()

	// Register this instance in the global registry
	registryMutex.Lock()
	processorRegistry[p.instanceID] = p
	registryMutex.Unlock()
	p.logger.Debug(ctx).Str("instance.id", p.instanceID).Msg("Processor instance registered") // Log instance ID here

	// Initial build and run of the Benthos stream using the configured processor YAML
	// updateBenthosStream handles locking internally
	err := p.updateBenthosStream(ctx, p.config.BenthosYAML)
	if err != nil {
		// Cleanup registry entry if initial build fails
		registryMutex.Lock()
		delete(processorRegistry, p.instanceID)
		registryMutex.Unlock()
		return fmt.Errorf("failed initial Benthos stream setup: %w", err)
	}

	p.logger.Info(ctx).Msg("Benthos processor opened successfully.")
	return nil
}

// buildAndRunBenthosStream encapsulates the logic to build and start a Benthos stream.
func (p *BenthosProcessor) buildAndRunBenthosStream(ctx context.Context, processorYAML string) (*service.Stream, context.CancelFunc, error) {
	p.logger.Debug(ctx).Msg("Building new Benthos stream instance...")

	builder := service.NewStreamBuilder()
	builder.DisableLinting() // Disable linting as we construct programmatically

	// Interpolate instance ID into the base config
	interpolatedBaseYAML := strings.ReplaceAll(baseBenthosConfigYAML, "${INSTANCE_ID}", p.instanceID)

	// Set the base configuration (input/output wrappers)
	err := builder.SetYAML(interpolatedBaseYAML)
	if err != nil {
		return nil, nil, fmt.Errorf("failed parsing base Benthos YAML config: %w", err)
	}

	// Add the processor-specific configuration
	if strings.TrimSpace(processorYAML) != "" {
		err = builder.AddProcessorYAML(processorYAML)
		if err != nil {
			return nil, nil, fmt.Errorf("failed parsing Benthos processor YAML config: %w", err)
		}
	} else {
		p.logger.Warn(ctx).Msg("No processor YAML provided, Benthos pipeline will have no processors.")
	}

	// Build the Benthos stream
	stream, err := builder.Build()
	if err != nil {
		return nil, nil, fmt.Errorf("failed building Benthos stream: %w", err)
	}

	// Run the Benthos stream in a goroutine
	// Use a background context so the stream's lifecycle isn't tied to this specific call context
	benthosCtx, cancelBenthos := context.WithCancel(context.Background())

	// Clear any stale error before starting the new stream goroutine
	select {
	case <-p.errC:
	default:
	}

	go func() {
		instanceLogger := p.logger.WithComponent("benthos.stream") // Logger for the stream goroutine
		instanceLogger.Info(benthosCtx).Msg("Running Benthos stream instance...")
		streamErr := stream.Run(benthosCtx) // This blocks until the stream stops
		if streamErr != nil && streamErr != context.Canceled {
			instanceLogger.Error(benthosCtx).Err(streamErr).Msg("Benthos stream instance exited with error")
			// Use non-blocking send to avoid deadlocking if the processor is already torn down
			select {
			case p.errC <- streamErr:
			default:
				instanceLogger.Warn(benthosCtx).Err(streamErr).Msg("Benthos stream error channel full or closed, dropping error")
			}
		} else if streamErr == context.Canceled {
			instanceLogger.Info(benthosCtx).Msg("Benthos stream instance shut down gracefully.")
		} else {
			// Should only happen if input closes gracefully AND stream isn't cancelled
			instanceLogger.Info(benthosCtx).Msg("Benthos stream instance finished.")
		}
	}()

	p.logger.Debug(ctx).Msg("New Benthos stream instance built and running.")
	return stream, cancelBenthos, nil
}

// UpdateBenthosStream handles stopping the current stream (if running)
// and starting a new one with the provided processor YAML configuration.
// This method is thread-safe and intended to be called externally.
func (p *BenthosProcessor) UpdateBenthosStream(ctx context.Context, newProcessorYAML string) error {
	p.mu.Lock() // Acquire exclusive lock for update
	defer p.mu.Unlock()

	p.logger.Info(ctx).Msg("Updating Benthos stream configuration...")

	// 1. Stop existing stream if it's running
	if p.cancelBenthos != nil {
		p.logger.Debug(ctx).Msg("Stopping existing Benthos stream instance...")
		p.cancelBenthos() // Signal the stream goroutine to stop
		p.cancelBenthos = nil
		p.benthosStream = nil
		// Note: We don't explicitly wait for the goroutine to finish,
		// cancellation should propagate. Starting the new stream is safe.
		p.logger.Debug(ctx).Msg("Existing Benthos stream instance stop signal sent.")
	}

	// 2. Build and run the new stream
	stream, cancel, err := p.buildAndRunBenthosStream(ctx, newProcessorYAML)
	if err != nil {
		p.logger.Error(ctx).Err(err).Msg("Failed to build and run new Benthos stream during update")
		// Keep the processor in a non-running state, don't update config
		return err
	}

	// 3. Update processor state with the new stream and config
	p.benthosStream = stream
	p.cancelBenthos = cancel
	p.config.BenthosYAML = newProcessorYAML // Store the currently active processor config

	p.logger.Info(ctx).Msg("Benthos stream configuration updated successfully.")
	return nil
}

// Alias for clarity in Open
func (p *BenthosProcessor) updateBenthosStream(ctx context.Context, newProcessorYAML string) error {
	return p.UpdateBenthosStream(ctx, newProcessorYAML)
}

func (p *BenthosProcessor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	// Use RLock for processing - allows multiple concurrent Process calls
	// if Benthos processing is fast enough, but blocks if an update is happening.
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Check if the stream is actually running (it might be during an update or failed to start)
	if p.benthosStream == nil || p.cancelBenthos == nil {
		p.logger.Warn(ctx).Msg("Benthos stream is not running, skipping processing batch")
		// Return errors for all records in the batch
		out := make([]sdk.ProcessedRecord, len(records))
		err := fmt.Errorf("Benthos stream not running (instance ID: %s)", p.instanceID)
		for i := range records {
			out[i] = sdk.ErrorRecord{Error: err}
		}
		return out
	}

	// If the stream is running, proceed with processing
	out := make([]sdk.ProcessedRecord, 0, len(records))
	for i, record := range records {
		// Process each record through Benthos
		// Note: processRecord handles its own locking internally if needed, but currently relies on the outer RLock
		processedRecord, err := p.processRecord(ctx, record)
		if err != nil {
			// If processRecord returns an error, wrap it in an sdk.ErrorRecord
			p.logger.Error(ctx).Err(err).Int("record_index", i).Msg("Failed processing record through Benthos")
			out = append(out, sdk.ErrorRecord{
				Error: fmt.Errorf("failed processing record %d: %w", i, err),
				// Optionally include the original record if needed for error handling downstream
				// Record: record,
			})
			// Decide if we should continue processing other records in the batch
			// For now, we continue, but you might want to return early on certain errors.
			continue
		}
		// If successful, wrap in sdk.SingleRecord
		out = append(out, sdk.SingleRecord(processedRecord))
	}

	if len(out) != len(records) {
		p.logger.Warn(ctx).Int("input_count", len(records)).Int("output_count", len(out)).Msg("Number of processed records does not match input count due to errors.")
	}

	return out
}

// processRecord handles sending a single record to Benthos and receiving the result.
// It assumes the caller holds at least a read lock (p.mu.RLock).
func (p *BenthosProcessor) processRecord(ctx context.Context, record opencdc.Record) (opencdc.Record, error) {
	// Double-check stream status under the lock, though the outer Process call should handle this.
	if p.benthosStream == nil {
		return opencdc.Record{}, fmt.Errorf("Benthos stream is not running (instance ID: %s)", p.instanceID)
	}

	// Send the record to Benthos input channel
	select {
	case p.records <- record:
		p.logger.Trace(ctx).RawJSON("record_key", record.Key.Bytes()).Msg("Record sent to Benthos input channel")
	case err := <-p.errC:
		p.logger.Error(ctx).Err(err).Msg("Received Benthos stream error while trying to send record")
		return opencdc.Record{}, fmt.Errorf("Benthos stream error: %w", err)
	case <-ctx.Done():
		p.logger.Warn(ctx).Msg("Context cancelled while trying to send record to Benthos")
		return opencdc.Record{}, ctx.Err()
	}

	// Wait for the processed record or an error from the Benthos output channel
	select {
	case result := <-p.results:
		if result.err != nil {
			p.logger.Error(ctx).Err(result.err).RawJSON("record_key", record.Key.Bytes()).Msg("Received processing error from Benthos output channel")
			return opencdc.Record{}, result.err // Propagate the processing error
		}
		p.logger.Trace(ctx).RawJSON("record_key", result.record.Key.Bytes()).Msg("Received processed record from Benthos output channel")
		return result.record, nil
	case err := <-p.errC:
		p.logger.Error(ctx).Err(err).Msg("Received Benthos stream error while waiting for result")
		return opencdc.Record{}, fmt.Errorf("Benthos stream error: %w", err)
	case <-ctx.Done():
		p.logger.Warn(ctx).Msg("Context cancelled while waiting for Benthos result")
		return opencdc.Record{}, ctx.Err()
	}
}

func (p *BenthosProcessor) Teardown(ctx context.Context) error {
	p.logger.Debug(ctx).Msg("Tearing down Benthos processor...")

	p.mu.Lock() // Acquire exclusive lock for teardown
	defer p.mu.Unlock()

	// Stop the Benthos stream if it's running
	if p.cancelBenthos != nil {
		p.logger.Debug(ctx).Msg("Stopping Benthos stream instance...")
		p.cancelBenthos()
		p.cancelBenthos = nil
		p.benthosStream = nil
		p.logger.Debug(ctx).Msg("Benthos stream instance stop signal sent.")
	}

	// Deregister this instance from the global registry
	if p.instanceID != "" {
		registryMutex.Lock()
		delete(processorRegistry, p.instanceID)
		registryMutex.Unlock()
		p.logger.Debug(ctx).Msg("Processor instance deregistered")
		p.instanceID = "" // Clear the ID
	}

	// Closing channels might cause panics if goroutines are still trying to use them.
	// Relying on context cancellation and GC is generally safer.
	// close(p.records)
	// close(p.results)
	// close(p.errC)

	p.logger.Info(ctx).Msg("Benthos processor torn down.")
	return nil
}

// --- Benthos service.Input Implementation (via wrapper) ---

func (w *conduitInputWrapper) Connect(ctx context.Context) error {
	w.logger.Debug(ctx).Msg("Benthos input connected")
	// No specific connection needed as we use channels
	return nil
}

func (w *conduitInputWrapper) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	// This Read method is called by the Benthos stream's input goroutine.
	// It reads from the specific processor instance's channel.
	select {
	case record := <-w.p.records:
		msg := w.p.toMessage(record) // Use helper from the processor instance
		w.logger.Trace(ctx).RawJSON("record_key", record.Key.Bytes()).Msg("Benthos input Read providing message")

		// The AckFunc captures the specific processor instance 'w.p'
		ackFn := func(ctx context.Context, err error) error {
			if err != nil {
				w.logger.Error(ctx).Err(err).RawJSON("record_key", record.Key.Bytes()).Msg("Benthos Nack received")
				// Send the error back to the processor's results channel
				// Use non-blocking send
				select {
				case w.p.results <- processResult{err: fmt.Errorf("benthos processing failed: %w", err)}:
				default:
					w.logger.Warn(ctx).Err(err).Msg("Result channel full or closed, dropping Nack error")
				}
			} else {
				w.logger.Trace(ctx).RawJSON("record_key", record.Key.Bytes()).Msg("Benthos Ack received (result sent via output Write)")
				// Success is handled by the Write method sending the result.
			}
			return nil
		}
		return msg, ackFn, nil

	case err := <-w.p.errC: // Check for fatal stream errors
		// Propagate the error to Benthos so it stops the input
		w.logger.Error(ctx).Err(err).Msg("Benthos stream error during Read")
		return nil, nil, fmt.Errorf("benthos stream error: %w", err)

	case <-ctx.Done(): // Context cancelled (Benthos stream shutting down)
		w.logger.Debug(ctx).Msg("Benthos input Read context cancelled")
		return nil, nil, ctx.Err() // Signal clean shutdown
	}
}

func (w *conduitInputWrapper) Close(ctx context.Context) error {
	w.logger.Debug(ctx).Msg("Benthos input closing")
	// No specific closing needed for channels from the input side
	return nil
}

// --- Benthos service.Output Implementation (via wrapper) ---

func (w *conduitOutputWrapper) Connect(ctx context.Context) error {
	w.logger.Debug(ctx).Msg("Benthos output connected")
	// No specific connection needed
	return nil
}

func (w *conduitOutputWrapper) Write(ctx context.Context, msg *service.Message) error {
	// This Write method is called by Benthos after processing is complete.
	// It writes the result back to the specific processor instance's channel.
	w.logger.Trace(ctx).Msg("Benthos output Write called")

	record, err := w.p.fromMessage(msg) // Use helper from the processor instance
	if err != nil {
		w.logger.Error(ctx).Err(err).Msg("Failed converting Benthos message to record in Write")
		// Send the conversion error back through the results channel
		// Use non-blocking send
		select {
		case w.p.results <- processResult{err: fmt.Errorf("failed converting Benthos message to record: %w", err)}:
		default:
			w.logger.Warn(ctx).Err(err).Msg("Result channel full or closed, dropping conversion error")
		}
		// Even though we sent the error back, we return nil to Benthos here
		// because the *output* operation itself didn't fail (we successfully received the message).
		// The error is related to *processing*, which is handled via the results channel and the input's AckFunc.
		// Returning an error here might cause Benthos to retry unnecessarily.
		return nil
	}

	w.logger.Trace(ctx).RawJSON("record_key", record.Key.Bytes()).Msg("Sending processed record from Benthos Write")
	// Send the successfully processed record back
	// Use non-blocking send
	select {
	case w.p.results <- processResult{record: record}:
		return nil // Success
	case <-ctx.Done():
		w.logger.Warn(ctx).Msg("Context cancelled while sending result from Benthos Write")
		return ctx.Err()
	default:
		// This case should ideally not happen if Process is waiting, but as a fallback:
		w.logger.Error(ctx).Msg("Result channel full or closed when sending success from Benthos Write")
		// We can't easily signal this back, Benthos might consider it a success.
		// This indicates a potential deadlock or issue in the processor logic.
		return fmt.Errorf("failed to send processed record back: result channel blocked")
	}
}

func (w *conduitOutputWrapper) Close(ctx context.Context) error {
	w.logger.Debug(ctx).Msg("Benthos output closing")
	// No specific closing needed
	return nil
}

// --- Helper methods ---

// toMessage converts an opencdc.Record to a Benthos message.
func (p *BenthosProcessor) toMessage(record opencdc.Record) *service.Message {
	// TODO: Potential optimization: If the record payload is already JSON bytes,
	// could we use msg.SetBytes(record.Payload.Bytes()) directly?
	// Need to ensure Benthos processors handle raw bytes correctly or if they expect structured.
	// For now, always convert to structured map for compatibility.
	msg := service.NewMessage(nil)
	msg.SetStructured(record.Map()) // Convert record to map[string]any
	return msg
}

// fromMessage converts a Benthos message back to an opencdc.Record.
func (p *BenthosProcessor) fromMessage(msg *service.Message) (opencdc.Record, error) {
	structured, err := msg.AsStructured()
	if err != nil {
		// Attempt to handle raw bytes if AsStructured fails
		// Fix: Correctly handle two return values from AsBytes
		if msgBytes, err := msg.AsBytes(); err == nil && msgBytes != nil {
			// This assumes the raw bytes represent the entire record structure,
			// which might not be the case depending on Benthos processors.
			// A common pattern might be processors modifying only payload.
			// This fallback needs careful consideration based on expected Benthos usage.
			p.logger.Warn(context.TODO()).Msg("Benthos message was not structured, attempting to treat raw bytes as record (experimental)")
			// We need a way to reconstruct the record from bytes. OpenCDC doesn't have a direct
			// method for this unless it's e.g., JSON bytes of the whole record map.
			// For now, return an error as this path is unclear.
			return opencdc.Record{}, fmt.Errorf("failed to get structured data and raw byte handling not fully implemented: %w", err)

		}
		return opencdc.Record{}, fmt.Errorf("failed to get structured data from Benthos message: %w", err)
	}

	structuredMap, ok := structured.(map[string]interface{})
	if !ok {
		// This might happen if Benthos outputs a non-map structure (e.g., just a string or number)
		return opencdc.Record{}, fmt.Errorf("Benthos message structured data was not a map[string]interface{}, got type %T", structured)
	}

	// Create record and populate it from the map data
	record := opencdc.Record{}
	// Unmap automatically handles converting map fields back to record fields
	err = record.Unmap(structuredMap)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to convert Benthos structured map to opencdc.Record: %w", err)
	}
	return record, nil
}
