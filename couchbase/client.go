package couchbase

import (
	"context"

	"github.com/Trendyol/go-dcp-couchbase/config"
	"github.com/Trendyol/go-dcp/couchbase"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type Client interface {
	Connect() error
	GetAgent() *gocbcore.Agent
	DeletePath(ctx context.Context,
		scopeName string,
		collectionName string,
		id []byte,
		path []byte,
	) error
	Close()
}

type client struct {
	agent  *gocbcore.Agent
	config *config.Couchbase
}

func (s *client) Connect() error {
	agent, err := couchbase.CreateAgent(
		s.config.Hosts, s.config.BucketName, s.config.Username, s.config.Password,
		s.config.SecureConnection, s.config.RootCAPath,
		s.config.ConnectionBufferSize, s.config.ConnectionTimeout,
	)
	if err != nil {
		return err
	}

	s.agent = agent

	logger.Log.Printf("connected to %s, bucket: %s", s.config.Hosts, s.config.BucketName)
	return nil
}

func (s *client) GetAgent() *gocbcore.Agent {
	return s.agent
}

func (s *client) DeletePath(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	path []byte,
) error {
	opm := couchbase.NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error)

	op, err := s.agent.MutateIn(gocbcore.MutateInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:   memd.SubDocOpDelete,
				Path: string(path),
			},
		},
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.MutateInResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	err = <-ch

	return err
}

func (s *client) Close() {
	_ = s.agent.Close()
	logger.Log.Printf("connections closed %s", s.config.Hosts)
}

func NewClient(config *config.Couchbase) Client {
	return &client{
		agent:  nil,
		config: config,
	}
}
