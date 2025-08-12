package couchbase

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp/helpers"

	"github.com/Trendyol/go-dcp/logger"

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
	dcpCheckpointCommit func()
	inflightCh          chan struct{}
	batchTicker         *time.Ticker
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
		requestTimeout:      config.Couchbase.RequestTimeout,
		dcpCheckpointCommit: dcpCheckpointCommit,
		metric:              &Metric{},
		sinkResponseHandler: sinkResponseHandler,
		targetClient:        targetClient,
		inflightCh:          make(chan struct{}, config.Couchbase.MaxInflightRequests),
		batchTicker:         time.NewTicker(config.Couchbase.BatchTickerDuration),
		batchSizeLimit:      config.Couchbase.BatchSizeLimit,
		batchByteSizeLimit:  helpers.ResolveUnionIntOrStringValue(config.Couchbase.BatchByteSizeLimit),
		batchTickerDuration: config.Couchbase.BatchTickerDuration,
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
	isRequestSuccessful := err == nil

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
		logger.Log.Error("error while write, err: %v", err)
		panic(err)
	}

	s := &SinkResponseHandlerContext{
		Action:       action,
		Err:          err,
		TargetClient: b.targetClient,
	}
	s.Retry = func(ctx context.Context) error {
		errCh := make(chan error, 1)

		b.inflightCh <- struct{}{}
		b.client.Execute(ctx, s.Action, func(err error) {
			errCh <- err
			<-b.inflightCh
		})

		return <-errCh
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
			errCh := make(chan error, 1)

			b.inflightCh <- struct{}{}
			b.client.Execute(ctx, s.Action, func(err error) {
				errCh <- err
				<-b.inflightCh
			})

			return <-errCh
		}
		b.sinkResponseHandler.OnSuccess(s)
	}
}

func (b *Processor) handleResponse(idx int, wg *sync.WaitGroup, err error) {
	b.panicOrGo(&b.batch[idx], err)
	wg.Done()
}

func (b *Processor) bulkRequest() {
	startedTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(len(b.batch))
	for i := 0; i < len(b.batch); i++ {
		func(idx int) {
			b.inflightCh <- struct{}{}
			b.client.Execute(ctx, &b.batch[idx], func(err error) {
				go b.handleResponse(idx, &wg, err)
				<-b.inflightCh
			})
		}(i)
	}
	wg.Wait()
	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()
	b.metric.BulkRequestSize = int64(b.batchSize)
	b.metric.BulkRequestByteSize = int64(b.batchByteSize)
}
