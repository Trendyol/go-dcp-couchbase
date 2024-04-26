package couchbase

import "time"

type Event struct {
	CollectionName string
	EventTime      time.Time
	Key            []byte
	Value          []byte
	Cas            uint64
	VbID           uint16
	IsDeleted      bool
	IsExpired      bool
	IsMutated      bool
	SeqNo          uint64
	RevNo          uint64
}

func NewDeleteEvent(
	key []byte, value []byte,
	collectionName string, eventTime time.Time, cas uint64, vbID uint16, seqNo uint64, revNo uint64,
) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsDeleted:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		Cas:            cas,
		VbID:           vbID,
		RevNo:          revNo,
		SeqNo:          seqNo,
	}
}

func NewExpireEvent(
	key []byte, value []byte,
	collectionName string, eventTime time.Time, cas uint64, vbID uint16, seqNo uint64, revNo uint64,
) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsExpired:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		Cas:            cas,
		VbID:           vbID,
		RevNo:          revNo,
		SeqNo:          seqNo,
	}
}

func NewMutateEvent(
	key []byte, value []byte,
	collectionName string, eventTime time.Time, cas uint64, vbID uint16, seqNo uint64, revNo uint64,
) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsMutated:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		Cas:            cas,
		VbID:           vbID,
		RevNo:          revNo,
		SeqNo:          seqNo,
	}
}
