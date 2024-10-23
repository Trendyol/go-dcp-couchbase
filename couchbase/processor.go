package couchbase

import (
	"context"
	"errors"
	"time"

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
	listenerCtxCh       chan *models.ListenerContext
	requestTimeout      time.Duration
	isDcpRebalancing    bool
}

type Metric struct {
	ProcessLatencyMs int64
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
		listenerCtxCh:       make(chan *models.ListenerContext, config.Couchbase.MaxInflightRequests),
	}

	return processor, nil
}

func (b *Processor) Close() {
	b.client.Close()
}

func (b *Processor) PrepareStartRebalancing() {
	b.isDcpRebalancing = true
}

func (b *Processor) PrepareEndRebalancing() {
	b.isDcpRebalancing = false
}

func (b *Processor) AddAction(
	listenerCtx *models.ListenerContext,
	eventTime time.Time,
	action *CBActionDocument,
) {
	b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()

	if b.isDcpRebalancing {
		return
	}

	b.listenerCtxCh <- listenerCtx
	ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)
	b.client.Execute(ctx, action, func(err error) {
		lCtx := <-b.listenerCtxCh
		cancel()
		go b.panicOrGo(lCtx, action, err)
	})
}

func (b *Processor) GetMetric() *Metric {
	return b.metric
}

func (b *Processor) panicOrGo(listenerCtx *models.ListenerContext, action *CBActionDocument, err error) {
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
		listenerCtx.Ack()
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

		b.client.Execute(ctx, s.Action, func(err error) {
			errCh <- err
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

			b.client.Execute(ctx, s.Action, func(err error) {
				errCh <- err
			})

			return <-errCh
		}
		b.sinkResponseHandler.OnSuccess(s)
	}
}
