package couchbase

import "github.com/Trendyol/go-dcp/tracing"

type EventContext struct {
	TargetClient
	Event
	tracing.ListenerTrace
}
