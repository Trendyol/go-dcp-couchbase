package couchbase

import (
	"context"

	"github.com/Trendyol/go-dcp-couchbase/config"

	"github.com/couchbase/gocbcore/v10"
)

type GetResult struct {
	Value []byte
	Cas   uint64
}
type GetCallback = func(*GetResult, error)

type TargetClient interface {
	Get(ctx context.Context, id []byte, cb GetCallback) error
}

type targetClient struct {
	agent          *gocbcore.Agent
	scopeName      string
	collectionName string
}

func (s *targetClient) Get(ctx context.Context,
	id []byte,
	cb GetCallback,
) error {
	deadline, _ := ctx.Deadline()

	_, err := s.agent.Get(gocbcore.GetOptions{
		Key:            id,
		Deadline:       deadline,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		RetryStrategy:  gocbcore.NewBestEffortRetryStrategy(nil),
	}, func(result *gocbcore.GetResult, err error) {
		if result == nil || err != nil {
			cb(nil, err)
		} else {
			cb(&GetResult{
				Value: result.Value,
				Cas:   uint64(result.Cas),
			}, err)
		}
	})

	return err
}

func NewTargetClient(config *config.Config, client Client) TargetClient {
	return &targetClient{
		agent:          client.GetAgent(),
		scopeName:      config.Couchbase.ScopeName,
		collectionName: config.Couchbase.CollectionName,
	}
}
