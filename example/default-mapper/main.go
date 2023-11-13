package main

import (
	"github.com/Trendyol/go-dcp-couchbase"
)

func main() {
	connector, err := dcpcouchbase.NewConnectorBuilder("config.yml").
		SetMapper(dcpcouchbase.DefaultMapper).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()

}
