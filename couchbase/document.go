package couchbase

type CbAction string

const (
	Set        CbAction = "Set"
	Delete     CbAction = "Delete"
	MutateIn   CbAction = "MutateIn"
	DeletePath CbAction = "DeletePath"
)

type CBActionDocument struct {
	Cas               *uint64
	Type              CbAction
	Source            []byte
	ID                []byte
	Path              []byte
	Size              int
	Expiry            uint32
	PreserveExpiry    bool
	DisableAutoCreate bool
}

func (doc *CBActionDocument) SetCas(cas uint64) {
	doc.Cas = &cas
}

func (doc *CBActionDocument) SetExpiry(expiry uint32) {
	doc.Expiry = expiry
}

func (doc *CBActionDocument) SetPreserveExpiry(preserveExpiry bool) {
	doc.PreserveExpiry = preserveExpiry
}

func (doc *CBActionDocument) SetDisableAutoCreate(value bool) {
	doc.DisableAutoCreate = value
}

func NewDeleteAction(key []byte) CBActionDocument {
	return CBActionDocument{
		ID:   key,
		Type: Delete,
		Size: len(key),
	}
}

func NewSetAction(key []byte, source []byte) CBActionDocument {
	return CBActionDocument{
		ID:     key,
		Source: source,
		Type:   Set,
		Size:   len(key) + len(source),
	}
}

func NewMutateInAction(key []byte, path []byte, source []byte) CBActionDocument {
	return CBActionDocument{
		ID:     key,
		Source: source,
		Type:   MutateIn,
		Path:   path,
		Size:   len(key) + len(path) + len(source),
	}
}

func NewDeletePathAction(key []byte, path []byte) CBActionDocument {
	return CBActionDocument{
		ID:   key,
		Type: DeletePath,
		Path: path,
		Size: len(key) + len(path),
	}
}
