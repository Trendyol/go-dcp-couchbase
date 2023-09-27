package couchbase

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"

	"github.com/Trendyol/go-dcp-couchbase/config"

	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
	"github.com/couchbase/gocbcore/v10"
)

type Processor struct {
	client              Client
	metric              *Metric
	agent               *gocbcore.Agent
	batchTicker         *time.Ticker
	dcpCheckpointCommit func()
	scopeName           string
	collectionName      string
	batch               []CBActionDocument
	batchSize           int
	requestTimeout      time.Duration
	batchTickerDuration time.Duration
	batchByteSizeLimit  int
	batchSizeLimit      int
	flushLock           sync.Mutex
	isDcpRebalancing    bool
}

type Metric struct {
	ProcessLatencyMs            int64
	BulkRequestProcessLatencyMs int64
}

func NewProcessor(config *config.Config,
	dcpCheckpointCommit func(),
) (*Processor, error) {
	client := NewClient(&config.Couchbase)
	err := client.Connect()
	if err != nil {
		return nil, err
	}

	processor := &Processor{
		client:              client,
		agent:               client.GetAgent(),
		batchTicker:         time.NewTicker(config.Couchbase.BatchTickerDuration),
		batchSizeLimit:      config.Couchbase.BatchSizeLimit,
		batchByteSizeLimit:  config.Couchbase.BatchByteSizeLimit,
		batchTickerDuration: config.Couchbase.BatchTickerDuration,
		requestTimeout:      config.Couchbase.RequestTimeout,
		scopeName:           config.Couchbase.ScopeName,
		collectionName:      config.Couchbase.CollectionName,
		dcpCheckpointCommit: dcpCheckpointCommit,
		metric:              &Metric{},
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
	b.client.Close()
}

func (b *Processor) flushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	if b.isDcpRebalancing {
		return
	}

	if len(b.batch) > 0 {
		b.bulkRequest()
		b.batchTicker.Reset(b.batchTickerDuration)
		b.batch = b.batch[:0]
		b.batchSize = 0
	}

	b.dcpCheckpointCommit()
}

func (b *Processor) PrepareStartRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = true
	b.batch = b.batch[:0]
	b.batchSize = 0
}

func (b *Processor) PrepareEndRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = false
}

func (b *Processor) AddActions(
	ctx *models.ListenerContext,
	eventTime time.Time,
	actions []CBActionDocument,
) {
	b.flushLock.Lock()
	b.batch = append(b.batch, actions...)
	b.batchSize += len(actions)
	ctx.Ack()
	b.flushLock.Unlock()

	b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()
	if b.batchSize >= b.batchSizeLimit || len(b.batch) >= b.batchByteSizeLimit {
		b.flushMessages()
	}
}

func (b *Processor) GetMetric() *Metric {
	return b.metric
}

func panicOrGo(err error, wg *sync.WaitGroup) {
	if err == nil {
		wg.Done()
		return
	}

	var keyValueErr *gocbcore.KeyValueError
	if errors.As(err, &keyValueErr) {
		wg.Done()
		return
	}

	panic(err)
}

func (b *Processor) bulkRequest() {
	startedTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(len(b.batch))

	for _, v := range b.batch {
		var err error

		switch {
		case v.Type == Set:
			err = b.client.CreateDocument(ctx, b.scopeName, b.collectionName, v.ID, v.Source, 0, 0,
				func(result *gocbcore.StoreResult, err error) {
					panicOrGo(err, &wg)
				})
		case v.Type == MutateIn:
			err = b.client.CreatePath(ctx, b.scopeName, b.collectionName, v.ID, v.Path, v.Source, memd.SubdocDocFlagMkDoc,
				func(result *gocbcore.MutateInResult, err error) {
					panicOrGo(err, &wg)
				})
		case v.Type == DeletePath:
			err = b.client.DeletePath(ctx, b.scopeName, b.collectionName, v.ID, v.Path,
				func(result *gocbcore.MutateInResult, err error) {
					panicOrGo(err, &wg)
				})
		case v.Type == Delete:
			err = b.client.DeleteDocument(ctx, b.scopeName, b.collectionName, v.ID,
				func(result *gocbcore.DeleteResult, err error) {
					panicOrGo(err, &wg)
				})
		default:
			logger.Log.Error("Unexpected action type: %v", v.Type)
		}

		if err != nil {
			panic(err)
		}
	}

	wg.Wait()
	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()
}
