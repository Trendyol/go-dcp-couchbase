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
	CreatePath(ctx context.Context,
		scopeName string,
		collectionName string,
		id []byte,
		path []byte,
		value []byte,
		flags memd.SubdocDocFlag,
		cb gocbcore.MutateInCallback,
	) error
	CreateDocument(ctx context.Context,
		scopeName string,
		collectionName string,
		id []byte,
		value []byte,
		flags uint32,
		expiry uint32,
		cb gocbcore.StoreCallback,
	) error
	DeleteDocument(ctx context.Context,
		scopeName string,
		collectionName string,
		id []byte,
		cb gocbcore.DeleteCallback,
	) error
	DeletePath(ctx context.Context,
		scopeName string,
		collectionName string,
		id []byte,
		path []byte,
		cb gocbcore.MutateInCallback,
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

func (s *client) CreatePath(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	path []byte,
	value []byte,
	flags memd.SubdocDocFlag,
	cb gocbcore.MutateInCallback,
) error {
	deadline, _ := ctx.Deadline()

	_, err := s.agent.MutateIn(gocbcore.MutateInOptions{
		Key:   id,
		Flags: flags,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Value: value,
				Path:  string(path),
			},
		},
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, cb)

	return err
}

func (s *client) CreateDocument(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	value []byte,
	flags uint32,
	expiry uint32,
	cb gocbcore.StoreCallback,
) error {
	deadline, _ := ctx.Deadline()

	_, err := s.agent.Set(gocbcore.SetOptions{
		Key:            id,
		Value:          value,
		Flags:          flags,
		Deadline:       deadline,
		Expiry:         expiry,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, cb)

	return err
}

func (s *client) DeleteDocument(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	cb gocbcore.DeleteCallback,
) error {
	deadline, _ := ctx.Deadline()

	_, err := s.agent.Delete(gocbcore.DeleteOptions{
		Key:            id,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, cb)

	return err
}

func (s *client) DeletePath(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	path []byte,
	cb gocbcore.MutateInCallback,
) error {
	deadline, _ := ctx.Deadline()

	_, err := s.agent.MutateIn(gocbcore.MutateInOptions{
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
	}, cb)

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
