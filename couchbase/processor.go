package couchbase

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp/helpers"

	"github.com/couchbase/gocbcore/v10/memd"

	"github.com/Trendyol/go-dcp-couchbase/config"

	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
	"github.com/couchbase/gocbcore/v10"
)

type Processor struct {
	sinkResponseHandler SinkResponseHandler
	client              Client
	metric              *Metric
	batchTicker         *time.Ticker
	dcpCheckpointCommit func()
	scopeName           string
	collectionName      string
	batch               []CBActionDocument
	requestTimeout      time.Duration
	batchTickerDuration time.Duration
	batchByteSizeLimit  int
	batchByteSize       int
	batchSizeLimit      int
	batchSize           int
	flushLock           sync.Mutex
	isDcpRebalancing    bool
}

type Metric struct {
	ProcessLatencyMs            int64
	BulkRequestProcessLatencyMs int64
	BulkRequestSize             int64
	BulkRequestByteSize         int64
}

func NewProcessor(
	config *config.Config,
	client Client,
	dcpCheckpointCommit func(),
	sinkResponseHandler SinkResponseHandler,
) (*Processor, error) {
	processor := &Processor{
		client:              client,
		batchTicker:         time.NewTicker(config.Couchbase.BatchTickerDuration),
		batchSizeLimit:      config.Couchbase.BatchSizeLimit,
		batchByteSizeLimit:  helpers.ResolveUnionIntOrStringValue(config.Couchbase.BatchByteSizeLimit),
		batchTickerDuration: config.Couchbase.BatchTickerDuration,
		requestTimeout:      config.Couchbase.RequestTimeout,
		scopeName:           config.Couchbase.ScopeName,
		collectionName:      config.Couchbase.CollectionName,
		dcpCheckpointCommit: dcpCheckpointCommit,
		metric:              &Metric{},
		sinkResponseHandler: sinkResponseHandler,
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
		b.batchByteSize = 0
	}

	b.dcpCheckpointCommit()
}

func (b *Processor) PrepareStartRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = true
	b.batch = b.batch[:0]
	b.batchSize = 0
	b.batchByteSize = 0
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
	isLastChunk bool,
) {
	b.flushLock.Lock()
	b.batch = append(b.batch, actions...)
	b.batchSize += len(actions)
	for _, action := range actions {
		b.batchByteSize += action.Size
	}
	if isLastChunk {
		ctx.Ack()
	}
	b.flushLock.Unlock()

	if isLastChunk {
		b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()
	}
	if b.batchSize >= b.batchSizeLimit || b.batchByteSize >= b.batchByteSizeLimit {
		b.flushMessages()
	}
}

func (b *Processor) GetMetric() *Metric {
	return b.metric
}

func (b *Processor) panicOrGo(action CBActionDocument, err error, wg *sync.WaitGroup) {
	defer wg.Done()

	isRequestSuccessful := false
	if err == nil {
		isRequestSuccessful = true
	}

	var kvErr *gocbcore.KeyValueError
	if errors.As(err, &kvErr) && (kvErr.StatusCode == memd.StatusKeyNotFound ||
		kvErr.StatusCode == memd.StatusSubDocPathNotFound ||
		kvErr.StatusCode == memd.StatusSubDocBadMulti ||
		kvErr.StatusCode == memd.StatusSubDocMultiPathFailureDeleted) {
		isRequestSuccessful = true
	}

	if isRequestSuccessful {
		b.handleSuccess(&action)
		return
	}

	b.handleError(&action, err)
}

func (b *Processor) handleError(action *CBActionDocument, err error) {
	if b.sinkResponseHandler == nil {
		panic(err)
	}

	b.sinkResponseHandler.OnError(&SinkResponseHandlerContext{
		Action: action,
		Err:    err,
	})
}

func (b *Processor) handleSuccess(action *CBActionDocument) {
	if b.sinkResponseHandler != nil {
		b.sinkResponseHandler.OnSuccess(&SinkResponseHandlerContext{
			Action: action,
		})
	}
}

func (b *Processor) bulkRequest() {
	startedTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(len(b.batch))

	for _, v := range b.batch {
		var err error

		casPtr := (*gocbcore.Cas)(v.Cas)

		switch {
		case v.Type == Set:
			err = b.client.CreateDocument(ctx, b.scopeName, b.collectionName, v.ID, v.Source, 0, 0,
				func(result *gocbcore.StoreResult, err error) {
					b.panicOrGo(v, err, &wg)
				})
		case v.Type == MutateIn:
			flags := memd.SubdocDocFlagMkDoc
			if v.DisableAutoCreate {
				flags = memd.SubdocDocFlagNone
			}

			err = b.client.CreatePath(ctx, b.scopeName, b.collectionName, v.ID, v.Path, v.Source, flags, casPtr,
				func(result *gocbcore.MutateInResult, err error) {
					b.panicOrGo(v, err, &wg)
				})
		case v.Type == DeletePath:
			err = b.client.DeletePath(ctx, b.scopeName, b.collectionName, v.ID, v.Path, casPtr,
				func(result *gocbcore.MutateInResult, err error) {
					b.panicOrGo(v, err, &wg)
				})
		case v.Type == Delete:
			err = b.client.DeleteDocument(ctx, b.scopeName, b.collectionName, v.ID, casPtr,
				func(result *gocbcore.DeleteResult, err error) {
					b.panicOrGo(v, err, &wg)
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
	b.metric.BulkRequestSize = int64(b.batchSize)
	b.metric.BulkRequestByteSize = int64(b.batchByteSize)
}
