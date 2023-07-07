package couchbase

import (
	"context"
	"crypto/x509"
	"os"
	"time"

	"github.com/Trendyol/go-dcp-client/couchbase"

	"github.com/Trendyol/go-couchbase-connect-couchbase/config"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/couchbase/gocbcore/v10"
)

type Client interface {
	Connect() error
	Close()
	CreateDocument(ctx context.Context, scopeName string, collectionName string, id []byte, value []byte, expiry uint32) error
	DeleteDocument(ctx context.Context, scopeName string, collectionName string, id []byte) error
}

type client struct {
	agent  *gocbcore.Agent
	config *config.Couchbase
}

func (s *client) Connect() error {
	agent, err := s.connect(s.config.BucketName, s.config.ConnectionBufferSize, s.config.ConnectionTimeout)
	if err != nil {
		return err
	}

	s.agent = agent

	logger.Log.Printf("connected to %s, bucket: %s", s.config.Hosts, s.config.BucketName)
	return nil
}

func (s *client) connect(bucketName string, connectionBufferSize uint, connectionTimeout time.Duration) (*gocbcore.Agent, error) {
	client, err := gocbcore.CreateAgent(
		&gocbcore.AgentConfig{
			BucketName: bucketName,
			SeedConfig: gocbcore.SeedConfig{
				HTTPAddrs: s.config.Hosts,
			},
			SecurityConfig: s.getSecurityConfig(),
			CompressionConfig: gocbcore.CompressionConfig{
				Enabled: true,
			},
			IoConfig: gocbcore.IoConfig{
				UseCollections: true,
			},
			KVConfig: gocbcore.KVConfig{
				ConnectionBufferSize: connectionBufferSize,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	ch := make(chan error)

	_, err = client.WaitUntilReady(
		time.Now().Add(connectionTimeout),
		gocbcore.WaitUntilReadyOptions{
			RetryStrategy: gocbcore.NewBestEffortRetryStrategy(nil),
		},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			ch <- err
		},
	)

	if err != nil {
		return nil, err
	}

	if err = <-ch; err != nil {
		return nil, err
	}

	return client, nil
}

func (s *client) Close() {
	_ = s.agent.Close()
	logger.Log.Printf("connections closed %s", s.config.Hosts)
}

func (s *client) getSecurityConfig() gocbcore.SecurityConfig {
	securityConfig := gocbcore.SecurityConfig{
		Auth: gocbcore.PasswordAuthProvider{
			Username: s.config.Username,
			Password: s.config.Password,
		},
	}

	if s.config.SecureConnection {
		securityConfig.UseTLS = true
		securityConfig.TLSRootCAProvider = s.tlsRootCaProvider
	}

	return securityConfig
}

func (s *client) tlsRootCaProvider() *x509.CertPool {
	cert, err := os.ReadFile(os.ExpandEnv(s.config.RootCAPath))
	if err != nil {
		logger.ErrorLog.Printf("error while reading cert file: %v", err)
		panic(err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(cert)

	return nil
}

func (s *client) CreateDocument(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	value []byte,
	expiry uint32,
) error {
	opm := couchbase.NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error)

	op, err := s.agent.Set(gocbcore.SetOptions{
		Key:            id,
		Value:          value,
		Deadline:       deadline,
		Expiry:         expiry,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.StoreResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	return <-ch
}

func (s *client) DeleteDocument(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
) error {
	opm := couchbase.NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	ch := make(chan error)

	op, err := s.agent.Delete(gocbcore.DeleteOptions{
		Key:            id,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.DeleteResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	return <-ch
}

func NewClient(config *config.Couchbase) Client {
	return &client{
		agent:  nil,
		config: config,
	}
}
