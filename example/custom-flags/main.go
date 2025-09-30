package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/couchbase/gocbcore/v10"

	dcpcouchbase "github.com/Trendyol/go-dcp-couchbase"
	"github.com/Trendyol/go-dcp-couchbase/couchbase"
)

func mapper(ctx couchbase.EventContext) []couchbase.CBActionDocument {
	// We are only interested in mutation events
	if !ctx.Event.IsMutated {
		return nil
	}

	// Create the flag for the JSON format.
	// gocbcore.JSONType is equal to gocbcore.DataType(1).
	jsonFlags := gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression)

	// Action to save the original document as JSON
	action := couchbase.NewSetAction(ctx.Event.Key, ctx.Event.Value)
	action.SetDocumentFlags(jsonFlags)

	// Create the flag for the Binary format.
	// gocbcore.BinaryType is equal to gocbcore.DataType(2).
	binaryFlags := gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression)

	// Action to save the raw content (value) of the document as binary with a different key
	backupKey := append([]byte("backup:"), ctx.Event.Key...)
	backupSetAction := couchbase.NewSetAction(backupKey, ctx.Event.Value)
	backupSetAction.SetDocumentFlags(binaryFlags)

	// Send both created actions to the processor
	return []couchbase.CBActionDocument{action, backupSetAction}
}

func main() {
	c, err := dcpcouchbase.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		Build()
	if err != nil {
		panic(err)
	}

	c.Start()
	defer c.Close()

	fmt.Println("Connector started")

	// Graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan
}
