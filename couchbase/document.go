package couchbase

type CbAction string

const (
	Index  CbAction = "Index"
	Delete CbAction = "Delete"
)

type CBActionDocument struct {
	Type   CbAction
	Source []byte
	ID     []byte
}

func NewDeleteAction(key []byte) CBActionDocument {
	return CBActionDocument{
		ID:   key,
		Type: Delete,
	}
}

func NewIndexAction(key []byte, source []byte) CBActionDocument {
	return CBActionDocument{
		ID:     key,
		Source: source,
		Type:   Index,
	}
}
