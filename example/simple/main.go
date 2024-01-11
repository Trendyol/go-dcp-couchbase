package main

import (
	"fmt"
	"github.com/Trendyol/go-dcp-couchbase"
	"github.com/Trendyol/go-dcp-couchbase/couchbase"
)

func main() {

	connector, err := dcpcouchbase.NewConnectorBuilder("config.yml").
		SetMapper(dcpcouchbase.DefaultMapper).
		SetSinkResponseHandler(&sinkResponseHandler{}).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()

}

type sinkResponseHandler struct {
}

func (s *sinkResponseHandler) OnSuccess(ctx *couchbase.SinkResponseHandlerContext) {
	fmt.Printf("OnSuccess %v\n", string(ctx.Action.Source))
}

func (s *sinkResponseHandler) OnError(ctx *couchbase.SinkResponseHandlerContext) {
	fmt.Printf("OnError %v\n", string(ctx.Action.Source))
}
