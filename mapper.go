package dcpcouchbase

import "github.com/Trendyol/go-dcp-couchbase/couchbase"

type Mapper func(ctx couchbase.EventContext) []couchbase.CBActionDocument

func DefaultMapper(ctx couchbase.EventContext) []couchbase.CBActionDocument {
	if ctx.IsMutated {
		return []couchbase.CBActionDocument{couchbase.NewSetAction(ctx.Key, ctx.Value)}
	}
	return []couchbase.CBActionDocument{couchbase.NewDeleteAction(ctx.Key)}
}
