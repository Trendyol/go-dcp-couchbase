package couchbase

type SinkResponseHandlerContext struct {
	Action *CBActionDocument
	Err    error
}

type SinkResponseHandler interface {
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
}
