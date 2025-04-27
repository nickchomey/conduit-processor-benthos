package benthos

import (
	"context"
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/matryer/is"
)

func TestBenthosProcessor_Configure(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		config  config.Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: config.Config{
				"benthosYAML": "uppercase",
			},
			wantErr: false,
		},
		{
			name:    "empty config",
			config:  config.Config{},
			wantErr: true,
		},
		{
			name: "empty benthosYAML",
			config: config.Config{
				"benthosYAML": "",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewProcessor()
			err := p.Configure(ctx, tt.config)
			if tt.wantErr {
				is.True(err != nil)
			} else {
				is.NoErr(err)
			}
		})
	}
}

func TestBenthosProcessor_Process(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	// Create a simple Benthos processor that uppercases the payload
	p := NewProcessor()
	err := p.Configure(ctx, config.Config{
		"benthosYAML": "uppercase",
	})
	is.NoErr(err)

	// Open the processor
	err = p.Open(ctx)
	is.NoErr(err)
	defer p.Teardown(ctx)

	// Create a test record
	record := opencdc.Record{
		Position:  opencdc.Position("test-position"),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{"key": "value"},
		Key:       opencdc.RawData("test-key"),
		Payload: opencdc.Change{
			After: opencdc.RawData("hello world"),
		},
	}

	// Process the record
	results := p.Process(ctx, []opencdc.Record{record})
	is.Equal(len(results), 1)

	// Check the result
	result, ok := results[0].(sdk.SingleRecord)
	is.True(ok)
	is.Equal(string(result.Position), "test-position")
	is.Equal(result.Operation, opencdc.OperationCreate)
	is.Equal(result.Metadata["key"], "value")
	is.Equal(result.Metadata["processed_by"], "benthos")
	is.Equal(string(result.Key.Bytes()), "test-key")
	is.Equal(string(result.Payload.After.Bytes()), "HELLO WORLD")
}

func TestBenthosProcessor_recordToMap_mapToRecord(t *testing.T) {
	is := is.New(t)
	p := NewProcessor()

	// Create a test record
	record := opencdc.Record{
		Position:  opencdc.Position("test-position"),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{"key": "value"},
		Key:       opencdc.RawData("test-key"),
		Payload: opencdc.Change{
			Before: opencdc.RawData("before-data"),
			After:  opencdc.RawData("after-data"),
		},
	}

	// Convert to map
	data, err := p.recordToMap(record)
	is.NoErr(err)
	is.True(data != nil)

	// Convert back to record
	convertedRecord, err := p.mapToRecord(data)
	is.NoErr(err)

	// Check the conversion
	is.Equal(string(convertedRecord.Position), "test-position")
	is.Equal(convertedRecord.Operation, opencdc.OperationCreate)
	is.Equal(convertedRecord.Metadata["key"], "value")
	is.Equal(string(convertedRecord.Key.Bytes()), "test-key")
	is.Equal(string(convertedRecord.Payload.Before.Bytes()), "before-data")
	is.Equal(string(convertedRecord.Payload.After.Bytes()), "after-data")
}
