package main

import (
	"github.com/Trendyol/go-dcp-couchbase"
)

func main() {
	connector, err := dcpcouchbase.NewConnector("config.yml", dcpcouchbase.DefaultMapper)
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()

}
