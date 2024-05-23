package couchbase

import "context"

type SinkResponseHandlerContext struct {
	TargetClient
	Action *CBActionDocument
	Retry  func(context.Context) error
	Err    error
}

type SinkResponseHandler interface {
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
}
