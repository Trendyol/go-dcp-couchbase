package couchbase

import (
	"context"
	"fmt"

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
		cas *gocbcore.Cas,
		expiry uint32,
		preserveExpiry bool,
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
		cas *gocbcore.Cas,
		cb gocbcore.DeleteCallback,
	) error
	DeletePath(ctx context.Context,
		scopeName string,
		collectionName string,
		id []byte,
		path []byte,
		cas *gocbcore.Cas,
		expiry uint32,
		preserveExpiry bool,
		cb gocbcore.MutateInCallback,
	) error
	Execute(ctx context.Context, action *CBActionDocument, callback func(err error))
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
		logger.Log.Error("error while connect to target bucket, err: %v", err)
		return err
	}

	s.agent = agent

	logger.Log.Info("connected to %s, target bucket: %s", s.config.Hosts, s.config.BucketName)
	return nil
}

func (s *client) GetAgent() *gocbcore.Agent {
	return s.agent
}

func (s *client) CreateMultiPath(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	pathValues []PathValue,
	flags memd.SubdocDocFlag,
	cas *gocbcore.Cas,
	expiry uint32,
	preserveExpiry bool,
	cb gocbcore.MutateInCallback,
) error {
	deadline, _ := ctx.Deadline()

	ops := make([]gocbcore.SubDocOp, len(pathValues))

	for i, pv := range pathValues {
		ops[i] = gocbcore.SubDocOp{
			Op:    memd.SubDocOpDictSet,
			Path:  string(pv.Path),
			Value: pv.Value,
		}
	}

	options := gocbcore.MutateInOptions{
		Key:            id,
		Flags:          flags,
		Ops:            ops,
		Expiry:         expiry,
		PreserveExpiry: preserveExpiry,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}

	if cas != nil {
		options.Cas = *cas
	}

	_, err := s.agent.MutateIn(options, cb)

	return err
}

func (s *client) CreatePath(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	path []byte,
	value []byte,
	flags memd.SubdocDocFlag,
	cas *gocbcore.Cas,
	expiry uint32,
	preserveExpiry bool,
	cb gocbcore.MutateInCallback,
) error {
	deadline, _ := ctx.Deadline()

	options := gocbcore.MutateInOptions{
		Key:   id,
		Flags: flags,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Value: value,
				Path:  string(path),
			},
		},
		Expiry:         expiry,
		PreserveExpiry: preserveExpiry,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}

	if cas != nil {
		options.Cas = *cas
	}

	_, err := s.agent.MutateIn(options, cb)

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
	cas *gocbcore.Cas,
	cb gocbcore.DeleteCallback,
) error {
	deadline, _ := ctx.Deadline()

	options := gocbcore.DeleteOptions{
		Key:            id,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}

	if cas != nil {
		options.Cas = *cas
	}

	_, err := s.agent.Delete(options, cb)

	return err
}

func (s *client) DeletePath(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	path []byte,
	cas *gocbcore.Cas,
	expiry uint32,
	preserveExpiry bool,
	cb gocbcore.MutateInCallback,
) error {
	deadline, _ := ctx.Deadline()

	options := gocbcore.MutateInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:   memd.SubDocOpDelete,
				Path: string(path),
			},
		},
		Expiry:         expiry,
		PreserveExpiry: preserveExpiry,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}

	if cas != nil {
		options.Cas = *cas
	}

	_, err := s.agent.MutateIn(options, cb)

	return err
}

func (s *client) Execute(ctx context.Context, action *CBActionDocument, callback func(error)) {
	var err error
	casPtr := (*gocbcore.Cas)(action.Cas)

	switch {
	case action.Type == Set:
		err = s.CreateDocument(ctx, s.config.ScopeName, s.config.CollectionName,
			action.ID, action.Source, 0, action.Expiry,
			func(result *gocbcore.StoreResult, err error) {
				callback(err)
			})
	case action.Type == MutateIn:
		flags := memd.SubdocDocFlagMkDoc
		if action.DisableAutoCreate {
			flags = memd.SubdocDocFlagNone
		}

		err = s.CreatePath(ctx, s.config.ScopeName, s.config.CollectionName,
			action.ID, action.Path, action.Source, flags, casPtr, action.Expiry, action.PreserveExpiry,
			func(result *gocbcore.MutateInResult, err error) {
				callback(err)
			})
	case action.Type == MultiMutateIn:
		flags := memd.SubdocDocFlagMkDoc
		if action.DisableAutoCreate {
			flags = memd.SubdocDocFlagNone
		}

		err = s.CreateMultiPath(ctx, s.config.ScopeName, s.config.CollectionName,
			action.ID, action.PathValues, flags, casPtr, action.Expiry, action.PreserveExpiry,
			func(result *gocbcore.MutateInResult, err error) {
				callback(err)
			})
	case action.Type == DeletePath:
		err = s.DeletePath(ctx, s.config.ScopeName, s.config.CollectionName,
			action.ID, action.Path, casPtr, action.Expiry, action.PreserveExpiry,
			func(result *gocbcore.MutateInResult, err error) {
				callback(err)
			})
	case action.Type == Delete:
		err = s.DeleteDocument(ctx, s.config.ScopeName, s.config.CollectionName,
			action.ID, casPtr,
			func(result *gocbcore.DeleteResult, err error) {
				callback(err)
			})
	default:
		err = fmt.Errorf("unexpected action type: %v", action.Type)
	}

	if err != nil {
		callback(err)
	}
}

func (s *client) Close() {
	_ = s.agent.Close()
	logger.Log.Info("connections closed %s", s.config.Hosts)
}

func NewClient(config *config.Couchbase) Client {
	return &client{
		agent:  nil,
		config: config,
	}
}
