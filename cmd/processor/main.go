//go:build wasm

package main

import (
	sdk "github.com/conduitio/conduit-processor-sdk"
	benthos "github.com/nickchomey/conduit-processor-benthos"
)

func main() {
	sdk.Run(benthos.NewProcessor())
}
