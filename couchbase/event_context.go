package couchbase

import "github.com/Trendyol/go-dcp/tracing"

type EventContext struct {
	TargetClient
	tracing.ListenerTrace
	Event
}
