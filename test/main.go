package main

import (
	"github.com/conduitio/conduit/cmd/conduit/cli"
	"github.com/conduitio/conduit/pkg/conduit"
	"github.com/conduitio/conduit/pkg/plugin/processor/builtin"
	benthos "github.com/nickchomey/conduit-processor-benthos"
)

func main() {
	// Get the default configuration, including all built-in connectors
	cfg := conduit.DefaultConfig()

	cfg.ProcessorPlugins["benthos"] = builtin.Constructor(benthos.NewBenthosProcessor)

	cli.Run(cfg)
}
