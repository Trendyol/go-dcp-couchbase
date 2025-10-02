package couchbase

type CbAction string

type PathValue struct {
	Path  []byte
	Value []byte
}

const (
	Set           CbAction = "Set"
	Delete        CbAction = "Delete"
	MutateIn      CbAction = "MutateIn"
	MultiMutateIn CbAction = "MultiMutateIn"
	DeletePath    CbAction = "DeletePath"
	ArrayAppend   CbAction = "ArrayAppend"
	Increment     CbAction = "Increment"
)

type CBActionDocument struct {
	Cas  *uint64
	Type CbAction
	// DocumentFlags specifies the common flags for a full-document operation (like Set).
	// It is used to control data format (JSON, binary, string) and compression.
	// The value should be generated using gocbcore.EncodeCommonFlags.
	// If left as 0 (default), the SDK will attempt to infer the data type.
	DocumentFlags     uint32
	PathValues        []PathValue
	Source            []byte
	ID                []byte
	Path              []byte
	Size              int
	Expiry            uint32
	PreserveExpiry    bool
	DisableAutoCreate bool
	Initial           uint64
	Delta             uint64
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

// SetDocumentFlags sets the common flags for a full-document operation.
// This allows for explicit control over the document's data format.
func (doc *CBActionDocument) SetDocumentFlags(flags uint32) *CBActionDocument {
	doc.DocumentFlags = flags
	return doc
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

func NewMultiMutateInAction(key []byte, pathValues []PathValue) CBActionDocument {
	size := len(key)
	for _, pv := range pathValues {
		size += len(pv.Path) + len(pv.Value)
	}

	return CBActionDocument{
		ID:         key,
		PathValues: pathValues,
		Type:       MultiMutateIn,
		Size:       size,
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

func NewIncrementAction(key []byte, initial uint64, delta uint64) CBActionDocument {
	return CBActionDocument{
		ID:      key,
		Type:    Increment,
		Initial: initial,
		Delta:   delta,
		Size:    len(key),
	}
}

func NewArrayAppendAction(key []byte, path []byte, source []byte) CBActionDocument {
	return CBActionDocument{
		ID:     key,
		Source: source,
		Type:   ArrayAppend,
		Path:   path,
		Size:   len(key) + len(path) + len(source),
	}
}
