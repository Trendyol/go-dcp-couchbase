package dcpcouchbase

import "github.com/Trendyol/go-dcp-couchbase/couchbase"

type Mapper func(event couchbase.Event) []couchbase.CBActionDocument

func DefaultMapper(event couchbase.Event) []couchbase.CBActionDocument {
	if event.IsMutated {
		return []couchbase.CBActionDocument{couchbase.NewSetAction(event.Key, event.Value)}
	}
	return []couchbase.CBActionDocument{couchbase.NewDeleteAction(event.Key)}
}
