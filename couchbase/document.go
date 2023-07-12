package couchbase

type CbAction string

const (
	Set      CbAction = "Set"
	Delete   CbAction = "Delete"
	MutateIn CbAction = "MutateIn"
)

type CBActionDocument struct {
	Type   CbAction
	Source []byte
	ID     []byte
	Path   []byte
}

func NewDeleteAction(key []byte) CBActionDocument {
	return CBActionDocument{
		ID:   key,
		Type: Delete,
	}
}

func NewSetAction(key []byte, source []byte) CBActionDocument {
	return CBActionDocument{
		ID:     key,
		Source: source,
		Type:   Set,
	}
}

func NewMutateInAction(key []byte, path []byte, source []byte) CBActionDocument {
	return CBActionDocument{
		ID:     key,
		Source: source,
		Type:   MutateIn,
		Path:   path,
	}
}
