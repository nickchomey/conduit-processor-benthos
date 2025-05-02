package benthos

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit/pkg/foundation/cerrors"

	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit/pkg/foundation/ctxutil"
	"github.com/conduitio/conduit/pkg/foundation/log"
	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
)

//go:generate paramgen -output=paramgen_proc.go BenthosConfig

// --- Benthos Component Wrappers and Registration ---

// conduitBenthosWrapper implements both service.BatchInput and service.BatchOutput interfaces.
// It acts as a bridge between Benthos and the Conduit processor.
type conduitBenthosWrapper struct {
	p      *BenthosProcessor
	logger log.CtxLogger
	role   string // "input" or "output"
}

// BenthosConfig represents the configuration for the Benthos processor
// It's used both for initial configuration and for updates
type BenthosConfig struct {
	// YAML is the complete Benthos configuration (excluding input/output)
	// This includes processors, resources, buffer, metrics, etc.
	YAML string `json:"yaml" validate:"required"`

	// BatchSize controls the maximum number of records to process in a single Benthos batch
	// Higher values can improve throughput but may increase memory usage
	BatchSize int `json:"batchSize" default:"100" validate:"gt=0"`

	// ChannelBufferSize controls the size of internal channels for communication
	// Higher values can improve throughput but use more memory
	ChannelBufferSize int `json:"channelBufferSize" default:"10"`

	// ThreadCount controls the number of parallel processing threads in the Benthos pipeline
	// Higher values can improve throughput for CPU-bound processors
	ThreadCount int `json:"threadCount" default:"1"`
}

type batchProcessResult struct {
	records []opencdc.Record
	err     error
}
type BenthosProcessor struct {
	sdk.UnimplementedProcessor

	config BenthosConfig

	// channels for communication with Benthos
	recordBatches chan []opencdc.Record
	resultBatches chan batchProcessResult
	errC          chan error // For receiving fatal errors from the Benthos stream goroutine

	// mutex to protect concurrent access during stream updates and processing
	// Use RWMutex: multiple readers (Process) can run concurrently,
	// but updates (updateBenthosStream, Teardown) need exclusive write lock.
	mu sync.RWMutex

	// Benthos stream
	benthosStream *service.Stream

	// Unique ID for this processor instance
	instanceID string

	// Logger instance
	logger log.CtxLogger
}

// --- Global Registry ---

// The processorRegistry is a global lookup table that maps processor IDs to processor instances.
// It serves two main purposes:
// 1. It allows Benthos input/output components to find the processor instance by ID
// 2. It allows external API calls (like UpdateBenthosStream) to find the processor to update its configuration
//
// Lifecycle:
// - Processors are registered in the Open method
// - Processors are deregistered in the Teardown method
//
// Note: We use ctxutil.ProcessorIDFromContext to get the processor ID from the context

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

// GetProcessorByID returns a BenthosProcessor instance by its ID.
// Returns the processor and a boolean indicating if it was found.
//
// This function is used by external API calls (like UpdateBenthosStream) to find
// a processor instance by its ID so that its configuration can be updated.
//
// The processor ID typically follows the format "pipelineID:processorID" and is
// set by Conduit when the processor is opened.
func GetProcessorByID(id string) (*BenthosProcessor, bool) {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	processor, ok := processorRegistry[id]
	return processor, ok
}

// Ensure Benthos input/output plugins are registered only once globally
func init() {
	// Create a shared config spec for both input and output components
	componentConfSpec := service.NewConfigSpec().
		Field(service.NewStringField("instance_id").Description("The unique ID of the processor instance."))

	err := service.RegisterBatchInput(
		"conduit_processor_input",
		componentConfSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			// Get the instance ID from the config
			instanceID, err := conf.FieldString("instance_id")
			if err != nil {
				return nil, cerrors.Errorf("failed to get instance_id for conduit_processor_input: %w", err)
			}
			if instanceID == "" {
				return nil, cerrors.New("instance_id is required for conduit_processor_input")
			}

			registryMutex.RLock()
			p, ok := processorRegistry[instanceID]
			registryMutex.RUnlock()

			if !ok {
				return nil, cerrors.Errorf("processor instance %q not found in registry", instanceID)
			}

			// Wrap with AutoRetryNacksBatched for automatic retry of failed batches
			return service.AutoRetryNacksBatched(&conduitBenthosWrapper{
				p:      p,
				logger: p.logger.WithComponent("benthos.input"),
				role:   "input",
			}), nil
		},
	)
	if err != nil {
		panic(fmt.Sprintf("failed registering Benthos batch input 'conduit_processor_input': %v", err))
	}

	err = service.RegisterBatchOutput(
		"conduit_processor_output",
		componentConfSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			// Get the instance ID from the config
			instanceID, err := conf.FieldString("instance_id")
			if err != nil {
				return nil, service.BatchPolicy{}, 0, cerrors.Errorf("failed to get instance_id for conduit_processor_output: %w", err)
			}
			if instanceID == "" {
				return nil, service.BatchPolicy{}, 0, cerrors.New("instance_id is required for conduit_processor_output")
			}

			registryMutex.RLock()
			p, ok := processorRegistry[instanceID]
			registryMutex.RUnlock()

			if !ok {
				return nil, service.BatchPolicy{}, 0, cerrors.Errorf("processor instance %q not found in registry", instanceID)
			}

			// Return the output wrapper with no batching policy (we handle batching ourselves)
			// and a max in-flight of 1 (we process one batch at a time)
			return &conduitBenthosWrapper{
				p:      p,
				logger: p.logger.WithComponent("benthos.output"),
				role:   "output",
			}, service.BatchPolicy{}, 1, nil
		},
	)
	if err != nil {
		panic(fmt.Sprintf("failed registering Benthos batch output 'conduit_processor_output': %v", err))
	}
}

// NewBenthosProcessor creates a new Benthos processor with the provided logger.
func NewBenthosProcessor(logger log.CtxLogger) *BenthosProcessor {
	// Default values - will be used if not overridden in Configure
	const defaultBatchSize = 100
	const defaultChannelBufferSize = 10

	return &BenthosProcessor{
		// All channels will be initialized in Configure
		logger: logger.WithComponent("processor.benthos"),
		// Default config values - will be overridden in Configure
		config: BenthosConfig{
			YAML:              "logger:\n  level: INFO",
			BatchSize:         defaultBatchSize,
			ChannelBufferSize: defaultChannelBufferSize,
			ThreadCount:       1,
		},
	}
}

func (p *BenthosProcessor) Specification() (sdk.Specification, error) {
	// Create a Benthos-style configuration specification
	spec := sdk.Specification{
		Name:        "benthos",
		Summary:     "Process records through a Benthos pipeline",
		Version:     "v0.1.0",
		Author:      "Conduit",
		Description: benthosProcessorDescription(),
		Parameters:  BenthosConfig{}.Parameters(),
	}

	// Add examples in the description since sdk.Specification doesn't have an Examples field

	return spec, nil
}

// benthosProcessorDescription returns a concise description of the Benthos processor
func benthosProcessorDescription() string {
	return `Integrates the Benthos stream processing library with Conduit, allowing you to leverage Benthos's extensive library of processors to transform, filter, and enrich your data.

Configure with YAML that defines a Benthos processing pipeline. Supports all Benthos processors including mapping, bloblang, json, filter, http, and more.

For detailed documentation and examples, see the README or visit the Benthos documentation at https://benthos.dev/docs/components/processors/about/`
}

func (p *BenthosProcessor) Configure(ctx context.Context, cfg config.Config) error {
	p.logger.Debug(ctx).Msg("Configuring Benthos processor...")

	// Parse and store the processor-specific YAML provided by the user/Conduit config
	err := sdk.ParseConfig(ctx, cfg, &p.config, BenthosConfig{}.Parameters())
	if err != nil {
		return cerrors.Errorf("failed to parse configuration: %w", err)
	}

	// Validate configuration values
	if p.config.BatchSize <= 0 {
		p.logger.Warn(ctx).Int("batch_size", p.config.BatchSize).Msg("Invalid batch size, using default value of 100")
		p.config.BatchSize = 100
	}

	if p.config.ChannelBufferSize <= 0 {
		p.logger.Warn(ctx).Int("channel_buffer_size", p.config.ChannelBufferSize).Msg("Invalid channel buffer size, using default value of 10")
		p.config.ChannelBufferSize = 10
	}

	if p.config.ThreadCount <= 0 {
		p.logger.Warn(ctx).Int("thread_count", p.config.ThreadCount).Msg("Invalid thread count, using default value of 1")
		p.config.ThreadCount = 1
	}

	// Ensure we have at least a basic logger configuration in the YAML
	if !strings.Contains(p.config.YAML, "logger:") {
		// Add a default logger configuration if none exists
		p.config.YAML = p.config.YAML + "\nlogger:\n  level: INFO"
	}

	// Initialize channels with the configured buffer size
	// This is safe during Configure as the processor isn't running yet
	p.recordBatches = make(chan []opencdc.Record, p.config.ChannelBufferSize)
	p.resultBatches = make(chan batchProcessResult, p.config.ChannelBufferSize)
	p.errC = make(chan error, 1) // Buffer of 1 for error channel

	p.logger.Debug(ctx).
		Int("channel_buffer_size", p.config.ChannelBufferSize).
		Msg("Initialized channels with configured buffer size")

	p.logger.Info(ctx).
		Str("yaml", p.config.YAML).
		Int("batchSize", p.config.BatchSize).
		Int("channelBufferSize", p.config.ChannelBufferSize).
		Int("threadCount", p.config.ThreadCount).
		Msg("Benthos processor configured")

	return nil
}

func (p *BenthosProcessor) Open(ctx context.Context) error {
	p.logger.Debug(ctx).Msg("Opening Benthos processor...")

	// Get the processor ID from the context
	// The processor ID is set by Conduit and follows the format "pipelineID:processorID"
	processorID := ctxutil.ProcessorIDFromContext(ctx)

	if processorID != "" {
		p.logger.Debug(ctx).Str("processorID", processorID).Msg("Found processor ID in context")
		p.instanceID = processorID
		p.logger.Info(ctx).Str("instance.id", p.instanceID).Msg("Using Conduit processor ID")
	} else {
		// If processor ID is not found in context, log a warning and fail
		p.logger.Error(ctx).Msg("Processor ID not found in context - this is required for the Benthos processor")
		return cerrors.New("processor ID not found in context - this is required for the Benthos processor")
	}

	// Register this instance in the global registry
	registryMutex.Lock()
	// Check if a processor with this ID already exists
	if _, exists := processorRegistry[p.instanceID]; exists {
		registryMutex.Unlock()
		p.logger.Warn(ctx).
			Str("instance.id", p.instanceID).
			Msg("A processor with this ID is already registered - this may indicate a duplicate processor ID")
		// We don't return an error here because the existing processor might be stale
		// Instead, we'll overwrite it and log a warning
	}

	// Register the processor
	processorRegistry[p.instanceID] = p
	registryCount := len(processorRegistry)
	registryMutex.Unlock()

	p.logger.Info(ctx).
		Str("instance.id", p.instanceID).
		Int("registry_count", registryCount).
		Msg("Processor instance registered in global registry")

	// Create a configuration for updating the Benthos stream
	config := BenthosConfig{
		YAML:        p.config.YAML,
		ThreadCount: p.config.ThreadCount,
	}

	// Initial build and run of the Benthos stream using the configured processor YAML
	// SetupBenthosStream handles locking internally
	err := p.SetupBenthosStream(ctx, config)
	if err != nil {
		// Cleanup registry entry if initial build fails
		registryMutex.Lock()
		delete(processorRegistry, p.instanceID)
		registryMutex.Unlock()
		return cerrors.Errorf("failed initial Benthos stream setup: %w", err)
	}

	p.logger.Info(ctx).Msg("Benthos processor opened successfully.")
	return nil
}

// SetupBenthosStream handles creating or updating the Benthos stream with the provided configuration.
// This method is thread-safe and can be called both during initialization and for runtime updates.
func (p *BenthosProcessor) SetupBenthosStream(ctx context.Context, config BenthosConfig) error {
	p.mu.Lock() // Acquire exclusive lock for update
	defer p.mu.Unlock()

	p.logger.Info(ctx).Msg("Setting up Benthos stream...")

	// 1. Stop existing stream if it's running
	if err := p.stopExistingStream(ctx); err != nil {
		return err
	}

	// 2. Build and run the new stream with the provided configuration
	builder := service.NewStreamBuilder()
	builder.DisableLinting() // Disable linting as we construct programmatically

	// Interpolate instance ID into the base config
	interpolatedBaseYAML := strings.ReplaceAll(baseBenthosConfigYAML, "${INSTANCE_ID}", p.instanceID)

	// Combine the base YAML (input/output) with the user's YAML
	completeYAML := interpolatedBaseYAML
	if config.YAML != "" {
		completeYAML = completeYAML + "\n" + config.YAML
	}

	// Log the complete YAML configuration for debugging
	p.logger.Debug(ctx).
		Str("instance_id", p.instanceID).
		Str("complete_yaml", completeYAML).
		Msg("Setting Benthos YAML configuration")

	// Set the complete configuration
	if err := builder.SetYAML(completeYAML); err != nil {
		p.logger.Error(ctx).
			Err(err).
			Str("instance_id", p.instanceID).
			Str("complete_yaml", completeYAML).
			Msg("Failed to parse Benthos YAML configuration")
		return cerrors.Errorf("failed parsing Benthos YAML config: %w", err)
	}

	// Set thread count for the pipeline (special case as it's not part of the YAML)
	threadCount := p.config.ThreadCount
	if config.ThreadCount > 0 {
		threadCount = config.ThreadCount
	}
	if threadCount > 1 {
		builder.SetThreads(threadCount)
	}

	// Build and run the stream
	stream, err := builder.Build()
	if err != nil {
		return cerrors.Errorf("failed building Benthos stream: %w", err)
	}

	// Run the stream in a background context
	benthosCtx := context.Background()
	go func() {
		instanceLogger := p.logger.WithComponent("benthos.stream")
		instanceLogger.Info(benthosCtx).Msg("Running Benthos stream instance...")
		streamErr := stream.Run(benthosCtx)
		if streamErr != nil && streamErr != context.Canceled {
			instanceLogger.Error(benthosCtx).Err(streamErr).Msg("Benthos stream instance exited with error")
			select {
			case p.errC <- streamErr:
			default:
				instanceLogger.Warn(benthosCtx).Err(streamErr).Msg("Benthos stream error channel full or closed, dropping error")
			}
		} else if streamErr == context.Canceled {
			instanceLogger.Info(benthosCtx).Msg("Benthos stream instance shut down gracefully.")
		} else {
			instanceLogger.Info(benthosCtx).Msg("Benthos stream instance finished.")
		}
	}()

	// Update processor state with the new stream
	p.benthosStream = stream

	// Store the YAML configuration for future reference
	if config.YAML != "" {
		p.config.YAML = config.YAML
	}

	// Update thread count if provided
	if config.ThreadCount > 0 {
		p.config.ThreadCount = config.ThreadCount
	}

	p.logger.Info(ctx).Msg("Benthos stream setup completed successfully")
	return nil
}

// stopExistingStream is a helper method to stop the current stream if it's running.
// It assumes the caller holds the mutex lock.
func (p *BenthosProcessor) stopExistingStream(ctx context.Context) error {
	if p.benthosStream != nil {
		p.logger.Debug(ctx).Msg("Stopping existing Benthos stream instance...")

		// Store the old stream
		oldStream := p.benthosStream

		// Clear the processor state immediately to prevent any new records from being processed
		p.benthosStream = nil

		// Clear any stale errors from the error channel
		select {
		case <-p.errC:
		default:
		}

		// Stop the old stream gracefully with a timeout
		stopCtx, stopCancel := context.WithTimeout(ctx, 5*time.Second)
		defer stopCancel()

		p.logger.Debug(ctx).Msg("Stopping Benthos stream gracefully...")
		err := oldStream.Stop(stopCtx)
		if err != nil {
			p.logger.Warn(ctx).Err(err).Msg("Error stopping Benthos stream, proceeding anyway")
		} else {
			p.logger.Debug(ctx).Msg("Benthos stream stopped gracefully")
		}

		p.logger.Debug(ctx).Msg("Existing Benthos stream instance cleanup complete")
	}
	return nil
}

func (p *BenthosProcessor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	// Use RLock for processing - allows multiple concurrent Process calls
	// if Benthos processing is fast enough, but blocks if an update is happening.
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Check if the stream is actually running (it might be during an update or failed to start)
	if p.benthosStream == nil {
		p.logger.Warn(ctx).Msg("Benthos stream is not running, skipping processing batch")
		// Return errors for all records in the batch
		out := make([]sdk.ProcessedRecord, len(records))
		err := cerrors.Errorf("Benthos stream not running (instance ID: %s)", p.instanceID)
		for i := range records {
			out[i] = sdk.ErrorRecord{Error: err}
		}
		return out
	}

	// Process records in batches according to the configured batch size
	out := make([]sdk.ProcessedRecord, 0, len(records))

	// Process records in batches
	for i := 0; i < len(records); i += p.config.BatchSize {
		// Calculate end index for current batch
		end := min(i+p.config.BatchSize, len(records))

		batchRecords := records[i:end]
		p.logger.Debug(ctx).Int("batch_size", len(batchRecords)).Int("start_index", i).Int("end_index", end-1).Msg("Processing batch")

		// Process this batch
		processedRecords, err := p.processBatch(ctx, batchRecords)
		if err != nil {
			// If batch processing fails, return errors for all records in this batch
			p.logger.Error(ctx).Err(err).Int("start_index", i).Int("end_index", end-1).Msg("Failed processing batch through Benthos")
			for j := range batchRecords {
				out = append(out, sdk.ErrorRecord{
					Error: cerrors.Errorf("batch processing failed (index %d): %w", i+j, err),
				})
			}
		} else {
			// Convert the processed records to sdk.ProcessedRecord format
			for _, record := range processedRecords {
				out = append(out, sdk.SingleRecord(record))
			}
		}
	}

	p.logger.Debug(ctx).
		Int("total_records", len(records)).
		Int("success_count", len(out)).
		Int("batch_size_config", p.config.BatchSize).
		Msg("All batches processing complete")

	if len(out) != len(records) {
		p.logger.Warn(ctx).Int("input_count", len(records)).Int("output_count", len(out)).Msg("Number of processed records does not match input count.")
	}

	return out
}

// processBatch handles sending a batch of records to Benthos and receiving the results.
// It assumes the caller holds at least a read lock (p.mu.RLock).
func (p *BenthosProcessor) processBatch(ctx context.Context, records []opencdc.Record) ([]opencdc.Record, error) {
	// Double-check stream status under the lock, though the outer Process call should handle this.
	if p.benthosStream == nil {
		return nil, cerrors.Errorf("Benthos stream is not running (instance ID: %s)", p.instanceID)
	}

	// Send the batch to Benthos input channel
	select {
	case p.recordBatches <- records:
		p.logger.Debug(ctx).Int("batch_size", len(records)).Msg("Record batch sent to Benthos input channel")
	case err := <-p.errC:
		p.logger.Error(ctx).Err(err).Msg("Received Benthos stream error while trying to send batch")
		return nil, cerrors.Errorf("Benthos stream error: %w", err)
	case <-ctx.Done():
		p.logger.Warn(ctx).Msg("Context cancelled while trying to send batch to Benthos")
		return nil, ctx.Err()
	}

	// Wait for the processed batch or an error from the Benthos output channel
	select {
	case result := <-p.resultBatches:
		if result.err != nil {
			p.logger.Error(ctx).Err(result.err).Msg("Received processing error from Benthos output channel")
			return nil, result.err // Propagate the processing error
		}
		p.logger.Debug(ctx).Int("result_size", len(result.records)).Msg("Received processed batch from Benthos output channel")
		return result.records, nil
	case err := <-p.errC:
		p.logger.Error(ctx).Err(err).Msg("Received Benthos stream error while waiting for batch result")
		return nil, cerrors.Errorf("Benthos stream error: %w", err)
	case <-ctx.Done():
		p.logger.Warn(ctx).Msg("Context cancelled while waiting for Benthos batch result")
		return nil, ctx.Err()
	}
}

func (p *BenthosProcessor) Teardown(ctx context.Context) error {
	p.logger.Debug(ctx).Msg("Tearing down Benthos processor...")

	p.mu.Lock() // Acquire exclusive lock for teardown
	defer p.mu.Unlock()

	// Stop the Benthos stream if it's running
	if p.benthosStream != nil {
		p.logger.Debug(ctx).Msg("Stopping Benthos stream instance gracefully...")

		// Stop the stream gracefully with a timeout
		stopCtx, stopCancel := context.WithTimeout(ctx, 5*time.Second)
		defer stopCancel()

		err := p.benthosStream.Stop(stopCtx)
		if err != nil {
			p.logger.Warn(ctx).Err(err).Msg("Error stopping Benthos stream during teardown")
		} else {
			p.logger.Debug(ctx).Msg("Benthos stream stopped gracefully during teardown")
		}

		p.benthosStream = nil
	}

	// Deregister this instance from the global registry
	if p.instanceID != "" {
		registryMutex.Lock()

		// Check if this processor is still in the registry
		// It's possible another processor with the same ID has replaced it
		if existing, exists := processorRegistry[p.instanceID]; exists {
			// Only delete if it's the same processor instance
			if existing == p {
				delete(processorRegistry, p.instanceID)
				registryCount := len(processorRegistry)
				p.logger.Info(ctx).
					Str("instance.id", p.instanceID).
					Int("registry_count", registryCount).
					Msg("Processor instance deregistered from global registry")
			} else {
				p.logger.Warn(ctx).
					Str("instance.id", p.instanceID).
					Msg("Not deregistering processor - a different processor with the same ID is now in the registry")
			}
		} else {
			p.logger.Warn(ctx).
				Str("instance.id", p.instanceID).
				Msg("Processor instance not found in registry during deregistration")
		}

		registryMutex.Unlock()
		p.instanceID = "" // Clear the ID regardless
	}

	p.logger.Info(ctx).Msg("Benthos processor torn down.")
	return nil
}

// --- Benthos service.BatchInput and service.BatchOutput Implementation (via wrapper) ---

// Connect implements both service.BatchInput.Connect and service.BatchOutput.Connect
func (w *conduitBenthosWrapper) Connect(ctx context.Context) error {
	w.logger.Debug(ctx).Str("role", w.role).Msg("Benthos component connected")
	// No specific connection needed as we use channels
	return nil
}

// ReadBatch implements service.BatchInput.ReadBatch
// This is only called when the wrapper is used as an input
func (w *conduitBenthosWrapper) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if w.role != "input" {
		w.logger.Error(ctx).Msg("ReadBatch called on wrapper with incorrect role")
		return nil, nil, cerrors.Errorf("ReadBatch called on wrapper with role %s", w.role)
	}

	// This ReadBatch method is called by the Benthos stream's input goroutine.
	// It reads from the specific processor instance's channel.
	select {
	case recordBatch := <-w.p.recordBatches:
		// Convert each record to a Benthos message
		messages := make(service.MessageBatch, len(recordBatch))
		for i, record := range recordBatch {
			messages[i] = w.p.toMessage(record)
		}

		w.logger.Debug(ctx).Int("batch_size", len(messages)).Msg("Benthos input ReadBatch providing message batch")

		// The AckFunc captures the specific processor instance 'w.p'
		ackFn := func(ctx context.Context, err error) error {
			if err != nil {
				w.logger.Error(ctx).Err(err).Int("batch_size", len(recordBatch)).Msg("Benthos Nack received for batch")
				// Send the error back to the processor's results channel
				// Use non-blocking send
				select {
				case w.p.resultBatches <- batchProcessResult{err: cerrors.Errorf("benthos batch processing failed: %w", err)}:
				default:
					w.logger.Warn(ctx).Err(err).Msg("Result channel full or closed, dropping Nack error")
				}
			} else {
				w.logger.Debug(ctx).Int("batch_size", len(recordBatch)).Msg("Benthos Ack received for batch (results sent via output WriteBatch)")
				// Success is handled by the WriteBatch method sending the results.
			}
			return nil
		}
		return messages, ackFn, nil

	case err := <-w.p.errC: // Check for fatal stream errors
		// Propagate the error to Benthos so it stops the input
		w.logger.Error(ctx).Err(err).Msg("Benthos stream error during ReadBatch")
		return nil, nil, cerrors.Errorf("benthos stream error: %w", err)

	case <-ctx.Done(): // Context cancelled (Benthos stream shutting down)
		w.logger.Debug(ctx).Msg("Benthos input ReadBatch context cancelled")
		return nil, nil, ctx.Err() // Signal clean shutdown
	}
}

// WriteBatch implements service.BatchOutput.WriteBatch
// This is only called when the wrapper is used as an output
func (w *conduitBenthosWrapper) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	if w.role != "output" {
		w.logger.Error(ctx).Msg("WriteBatch called on wrapper with incorrect role")
		return cerrors.Errorf("WriteBatch called on wrapper with role %s", w.role)
	}

	// This WriteBatch method is called by Benthos after processing is complete.
	// It writes the results back to the specific processor instance's channel.
	w.logger.Debug(ctx).Int("batch_size", len(msgs)).Msg("Benthos output WriteBatch called")

	// Convert each message back to a record
	records := make([]opencdc.Record, 0, len(msgs))
	var conversionErr error

	for i, msg := range msgs {
		record, err := w.p.fromMessage(ctx, msg)
		if err != nil {
			w.logger.Error(ctx).Err(err).Int("msg_index", i).Msg("Failed converting Benthos message to record in WriteBatch")
			conversionErr = cerrors.Errorf("failed converting message %d: %w", i, err)
			break
		}
		records = append(records, record)
	}

	// If there was an error converting any message, send the error back
	if conversionErr != nil {
		select {
		case w.p.resultBatches <- batchProcessResult{err: conversionErr}:
		default:
			w.logger.Warn(ctx).Err(conversionErr).Msg("Result channel full or closed, dropping conversion error")
		}
		// Even though we sent the error back, we return nil to Benthos here
		// because the *output* operation itself didn't fail (we successfully received the messages).
		return nil
	}

	w.logger.Debug(ctx).Int("record_count", len(records)).Msg("Sending processed records from Benthos WriteBatch")
	// Send the successfully processed records back
	// Use non-blocking send
	select {
	case w.p.resultBatches <- batchProcessResult{records: records}:
		return nil // Success
	case <-ctx.Done():
		w.logger.Warn(ctx).Msg("Context cancelled while sending results from Benthos WriteBatch")
		return ctx.Err()
	default:
		// This case should ideally not happen if Process is waiting, but as a fallback:
		w.logger.Error(ctx).Msg("Result channel full or closed when sending success from Benthos WriteBatch")
		// We can't easily signal this back, Benthos might consider it a success.
		return cerrors.Errorf("failed to send processed records back: result channel blocked")
	}
}

// Close implements both service.BatchInput.Close and service.BatchOutput.Close
func (w *conduitBenthosWrapper) Close(ctx context.Context) error {
	w.logger.Debug(ctx).Str("role", w.role).Msg("Benthos component closing")
	// No specific closing needed
	return nil
}

// --- Helper methods ---

// toMessage converts an opencdc.Record to a Benthos message.
func (p *BenthosProcessor) toMessage(record opencdc.Record) *service.Message {
	// Create a new message with nil content
	msg := service.NewMessage(nil)

	// Convert record to structured map representation
	recordMap := record.Map()

	// Set the structured data on the message
	msg.SetStructured(recordMap)

	return msg
}

// fromMessage converts a Benthos message back to an opencdc.Record.
func (p *BenthosProcessor) fromMessage(ctx context.Context, msg *service.Message) (opencdc.Record, error) {
	// Get structured data from the message
	structured, err := msg.AsStructured()
	if err != nil {
		// Attempt to handle raw bytes if AsStructured fails
		p.logger.Debug(ctx).Err(err).Msg("Failed to get structured data from Benthos message, attempting to handle raw bytes")

		msgBytes, bytesErr := msg.AsBytes()
		if bytesErr != nil {
			p.logger.Error(ctx).Err(bytesErr).Msg("Failed to get raw bytes from Benthos message")
			return opencdc.Record{}, fmt.Errorf("failed to get data from Benthos message: could not get structured data (%w) or raw bytes (%v)", err, bytesErr)
		}

		if msgBytes == nil {
			p.logger.Error(ctx).Msg("Benthos message contains neither structured data nor raw bytes")
			return opencdc.Record{}, fmt.Errorf("failed to get data from Benthos message: message contains neither structured data nor raw bytes")
		}

		// Try to parse bytes as JSON (assuming it's a JSON representation of a record)
		var structuredMap map[string]interface{}
		if jsonErr := json.Unmarshal(msgBytes, &structuredMap); jsonErr == nil {
			// If JSON parsing succeeded, use the structured map
			p.logger.Debug(ctx).Msg("Successfully parsed raw bytes as JSON map")
			structured = structuredMap
		} else {
			// If not JSON, create a simple record with the raw bytes as payload.after
			p.logger.Debug(ctx).
				Int("bytes_length", len(msgBytes)).
				Msg("Raw bytes are not JSON, creating simple record with raw payload")

			// Create a new record directly
			return opencdc.Record{
				Payload: opencdc.Change{
					After: opencdc.RawData(msgBytes),
				},
			}, nil
		}
	}

	structuredMap, ok := structured.(map[string]interface{})
	if !ok {
		// This might happen if Benthos outputs a non-map structure (e.g., just a string or number)
		p.logger.Error(ctx).
			Str("type", fmt.Sprintf("%T", structured)).
			Str("value", fmt.Sprintf("%v", structured)).
			Msg("Benthos message structured data was not a map[string]interface{}")

		return opencdc.Record{}, fmt.Errorf("Benthos message structured data was not a map[string]interface{}, got type %T with value: %v", structured, structured)
	}

	// Create a new record
	record := opencdc.Record{}

	// Unmap automatically handles converting map fields back to record fields
	err = record.Unmap(structuredMap)
	if err != nil {
		// Include more details about the map in the error message
		keys := make([]string, 0, len(structuredMap))
		for k := range structuredMap {
			keys = append(keys, k)
		}

		p.logger.Error(ctx).
			Err(err).
			Strs("map_keys", keys).
			Msg("Failed to convert Benthos structured map to opencdc.Record")

		return opencdc.Record{}, fmt.Errorf("failed to convert Benthos structured map to opencdc.Record: %w (map keys: %v)", err, keys)
	}

	p.logger.Debug(ctx).Msg("Successfully converted Benthos message to opencdc.Record")
	return record, nil
}
