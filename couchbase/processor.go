package couchbase

import (
	"context"
	"errors"
	"time"

	"github.com/Trendyol/go-dcp-couchbase/config"

	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/models"
	"github.com/couchbase/gocbcore/v10"
)

type Processor struct {
	cbClient            Client
	logger              logger.Logger
	errorLogger         logger.Logger
	batchTicker         *time.Ticker
	dcpCheckpointCommit func()
	scopeName           string
	collectionName      string
	batch               []CBActionDocument
	batchSize           int
	requestTimeoutMs    int
	batchTickerDuration time.Duration
	batchByteSizeLimit  int
	batchSizeLimit      int
}

func NewProcessor(config *config.Config,
	logger logger.Logger,
	errorLogger logger.Logger,
	dcpCheckpointCommit func(),
) (*Processor, error) {
	cbClient := NewClient(&config.Couchbase)
	err := cbClient.Connect()
	if err != nil {
		return nil, err
	}

	processor := &Processor{
		cbClient:            cbClient,
		batchTicker:         time.NewTicker(config.Couchbase.BatchTickerDuration),
		batchSizeLimit:      config.Couchbase.BatchSizeLimit,
		batchByteSizeLimit:  config.Couchbase.BatchByteSizeLimit,
		batchTickerDuration: config.Couchbase.BatchTickerDuration,
		requestTimeoutMs:    config.Couchbase.RequestTimeoutMs,
		scopeName:           config.Couchbase.ScopeName,
		collectionName:      config.Couchbase.CollectionName,
		dcpCheckpointCommit: dcpCheckpointCommit,
		logger:              logger,
		errorLogger:         errorLogger,
	}

	return processor, nil
}

func (b *Processor) StartProcessor() {
	for range b.batchTicker.C {
		b.flushMessages()
	}
}

func (b *Processor) Close() {
	b.batchTicker.Stop()

	b.flushMessages()
	b.cbClient.Close()
}

func (b *Processor) flushMessages() {
	if len(b.batch) > 0 {
		err := b.bulkRequest()
		if err != nil {
			panic(err)
		}
		b.batchTicker.Reset(b.batchTickerDuration)
		b.batch = b.batch[:0]
		b.batchSize = 0
	}

	b.dcpCheckpointCommit()
}

func (b *Processor) AddActions(
	ctx *models.ListenerContext,
	_ time.Time,
	actions []CBActionDocument,
	_ string,
) {
	b.batch = append(b.batch, actions...)
	b.batchSize += len(actions)
	ctx.Ack()

	// TODO: metric
	if b.batchSize >= b.batchSizeLimit || len(b.batch) >= b.batchByteSizeLimit {
		b.flushMessages()
	}
}

func (b *Processor) bulkRequest() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.requestTimeoutMs)*time.Millisecond)
	defer cancel()
	for _, v := range b.batch {
		if v.Type == Index {
			err := b.cbClient.CreateDocument(ctx, b.scopeName, b.collectionName, v.ID, v.Source, 0)
			if err != nil {
				return err
			}
		} else {
			err := b.cbClient.DeleteDocument(ctx, b.scopeName, b.collectionName, v.ID)
			var keyValueErr *gocbcore.KeyValueError
			if errors.As(err, &keyValueErr) {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}
