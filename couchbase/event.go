package couchbase

import (
	"github.com/Trendyol/go-dcp/models"
	"time"
)

type Event struct {
	CollectionName string
	Key            []byte
	Value          []byte
	IsDeleted      bool
	IsExpired      bool
	IsMutated      bool
	Metadata       Metadata
}

type Metadata struct {
	Cas          uint64
	VbID         uint16
	EventTime    time.Time
	SeqNo        uint64
	RevNo        uint64
	Flags        uint32
	Expiry       uint32
	LockTime     uint32
	CollectionID uint32
	StreamID     uint16
	DataType     uint8
}

func NewDeleteEvent(key []byte, value []byte, collectionName string, metadata Metadata) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsDeleted:      true,
		CollectionName: collectionName,
		Metadata:       metadata,
	}
}

func NewExpireEvent(key []byte, value []byte, collectionName string, metadata Metadata) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsExpired:      true,
		CollectionName: collectionName,
		Metadata:       metadata,
	}
}

func NewMutateEvent(key []byte, value []byte, collectionName string, metadata Metadata) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsMutated:      true,
		CollectionName: collectionName,
		Metadata:       metadata,
	}
}

func NewDeleteMetadata(event models.DcpDeletion) Metadata {
	return Metadata{
		Cas:          event.Cas,
		VbID:         event.VbID,
		EventTime:    event.EventTime,
		SeqNo:        event.SeqNo,
		RevNo:        event.RevNo,
		CollectionID: event.CollectionID,
		StreamID:     event.StreamID,
		DataType:     event.Datatype,
	}
}

func NewExpireMetadata(event models.DcpExpiration) Metadata {
	return Metadata{
		Cas:          event.Cas,
		VbID:         event.VbID,
		EventTime:    event.EventTime,
		SeqNo:        event.SeqNo,
		RevNo:        event.RevNo,
		CollectionID: event.CollectionID,
		StreamID:     event.StreamID,
	}
}

func NewMutateMetadata(event models.DcpMutation) Metadata {
	return Metadata{
		Cas:          event.Cas,
		VbID:         event.VbID,
		EventTime:    event.EventTime,
		SeqNo:        event.SeqNo,
		RevNo:        event.RevNo,
		Flags:        event.Flags,
		Expiry:       event.Expiry,
		LockTime:     event.LockTime,
		CollectionID: event.CollectionID,
		StreamID:     event.StreamID,
		DataType:     event.Datatype,
	}
}
