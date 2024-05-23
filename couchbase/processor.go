package couchbase

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp/helpers"

	"github.com/couchbase/gocbcore/v10/memd"

	"github.com/Trendyol/go-dcp-couchbase/config"

	"github.com/Trendyol/go-dcp/models"
	"github.com/couchbase/gocbcore/v10"
)

type Processor struct {
	sinkResponseHandler SinkResponseHandler
	targetClient        TargetClient
	client              Client
	metric              *Metric
	batchTicker         *time.Ticker
	dcpCheckpointCommit func()
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
	targetClient TargetClient,
) (*Processor, error) {
	processor := &Processor{
		client:              client,
		batchTicker:         time.NewTicker(config.Couchbase.BatchTickerDuration),
		batchSizeLimit:      config.Couchbase.BatchSizeLimit,
		batchByteSizeLimit:  helpers.ResolveUnionIntOrStringValue(config.Couchbase.BatchByteSizeLimit),
		batchTickerDuration: config.Couchbase.BatchTickerDuration,
		requestTimeout:      config.Couchbase.RequestTimeout,
		dcpCheckpointCommit: dcpCheckpointCommit,
		metric:              &Metric{},
		sinkResponseHandler: sinkResponseHandler,
		targetClient:        targetClient,
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

func (b *Processor) panicOrGo(action *CBActionDocument, err error) {
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
		b.handleSuccess(action)
		return
	}

	b.handleError(action, err)
}

func (b *Processor) handleError(action *CBActionDocument, err error) {
	if b.sinkResponseHandler == nil {
		panic(err)
	}

	s := &SinkResponseHandlerContext{
		Action:       action,
		Err:          err,
		TargetClient: b.targetClient,
	}
	s.Retry = func(ctx context.Context) error {
		return b.client.Execute(ctx, s.Action)
	}
	b.sinkResponseHandler.OnError(s)
}

func (b *Processor) handleSuccess(action *CBActionDocument) {
	if b.sinkResponseHandler != nil {
		s := &SinkResponseHandlerContext{
			Action:       action,
			TargetClient: b.targetClient,
		}
		s.Retry = func(ctx context.Context) error {
			return b.client.Execute(ctx, s.Action)
		}
		b.sinkResponseHandler.OnSuccess(s)
	}
}

func (b *Processor) bulkRequest() {
	startedTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(len(b.batch))

	for _, v := range b.batch {
		err := b.client.Execute(ctx, &v) //nolint:gosec
		b.panicOrGo(&v, err)             //nolint:gosec
		wg.Done()
	}

	wg.Wait()
	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()
	b.metric.BulkRequestSize = int64(b.batchSize)
	b.metric.BulkRequestByteSize = int64(b.batchByteSize)
}
