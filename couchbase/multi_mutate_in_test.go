package couchbase

import (
	"reflect"
	"testing"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

func mixedSubdocOps() []gocbcore.SubDocOp {
	return []gocbcore.SubDocOp{
		{Op: memd.SubDocOpDelete, Path: "entries.`id.with.dots`"},
		{
			Op: memd.SubDocOpDictSet, Path: "lastModifiedDates.processed",
			Value: []byte(`"2026-01-02T03:04:05.123Z"`), Flags: memd.SubdocFlagMkDirP,
		},
	}
}

func TestNewMultiMutateInActionWithOps(t *testing.T) {
	key := []byte("document-ç")
	ops := mixedSubdocOps()
	action := NewMultiMutateInActionWithOps(key, ops)
	if action.Type != MultiMutateIn || !reflect.DeepEqual(action.ID, key) || !reflect.DeepEqual(action.SubDocOps, ops) {
		t.Fatalf("action did not retain its key and mixed operations: %+v", action)
	}
	wantSize := len(key)
	for _, op := range ops {
		wantSize += len(op.Path) + len(op.Value)
	}
	if action.Size != wantSize {
		t.Fatalf("batch byte size: got %d, want %d", action.Size, wantSize)
	}
	if action.Cas != nil || action.Expiry != 0 || action.PreserveExpiry || action.DisableAutoCreate {
		t.Fatalf("unexpected mutation defaults: %+v", action)
	}
	if len(action.PathValues) != 0 || len(action.Source) != 0 || len(action.Path) != 0 {
		t.Fatal("mixed operations must not be represented as legacy actions")
	}
}

func TestLegacyMultiMutateInContract(t *testing.T) {
	// Keep the original two-field positional literal source-compatible as well.
	values := []PathValue{{[]byte("entries.id"), []byte(`{"value":1}`)}}
	action := NewMultiMutateInAction([]byte("document"), values)
	if action.Type != MultiMutateIn || string(action.Type) != "MultiMutateIn" || action.SubDocOps != nil {
		t.Fatalf("legacy action contract changed: %+v", action)
	}
	if !reflect.DeepEqual(action.PathValues, values) || action.Size != len("documententries.id"+`{"value":1}`) {
		t.Fatal("legacy payload or batch size changed")
	}
}
