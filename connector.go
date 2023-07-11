package dcpcouchbase

import (
	"errors"

	"github.com/Trendyol/go-dcp-couchbase/config"
	"github.com/Trendyol/go-dcp-couchbase/couchbase"

	"github.com/Trendyol/go-dcp"
	dcpClientConfig "github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
)

type Connector interface {
	Start()
	Close()
}

type connector struct {
	dcp         dcp.Dcp
	config      *config.Config
	mapper      Mapper
	logger      logger.Logger
	errorLogger logger.Logger
	processor   *couchbase.Processor
}

func (c *connector) Start() {
	go func() {
		<-c.dcp.WaitUntilReady()
		c.processor.StartProcessor()
	}()
	c.dcp.Start()
}

func (c *connector) Close() {
	c.dcp.Close()
	c.processor.Close()
}

func (c *connector) listener(ctx *models.ListenerContext) {
	var e couchbase.Event
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		e = couchbase.NewMutateEvent(event.Key, event.Value, event.CollectionName, event.EventTime)
	case models.DcpExpiration:
		e = couchbase.NewExpireEvent(event.Key, nil, event.CollectionName, event.EventTime)
	case models.DcpDeletion:
		e = couchbase.NewDeleteEvent(event.Key, nil, event.CollectionName, event.EventTime)
	default:
		return
	}

	actions := c.mapper(e)

	if len(actions) == 0 {
		ctx.Ack()
		return
	}

	c.processor.AddActions(ctx, e.EventTime, actions, e.CollectionName)
}

func createDcp(cfg any, listener models.Listener, logger logger.Logger, errorLogger logger.Logger) (dcp.Dcp, error) {
	switch v := cfg.(type) {
	case dcpClientConfig.Dcp:
		return dcp.NewDcpWithLoggers(v, listener, logger, errorLogger)
	case string:
		return dcp.NewDcpWithLoggers(v, listener, logger, errorLogger)
	default:
		return nil, errors.New("invalid config")
	}
}

func NewConnector(cf any, mapper Mapper, logger logger.Logger, errorLogger logger.Logger) (Connector, error) {
	cfg, err := newConfig(cf)
	if err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()

	connector := &connector{
		mapper:      mapper,
		config:      cfg,
		logger:      logger,
		errorLogger: errorLogger,
	}

	if err != nil {
		return nil, err
	}

	dcp, err := createDcp(cfg.Dcp, connector.listener, logger, errorLogger)
	if err != nil {
		connector.errorLogger.Printf("dcp error: %v", err)
		return nil, err
	}

	dcpConfig := dcp.GetConfig()
	dcpConfig.Checkpoint.Type = "manual"

	connector.dcp = dcp
	processor, err := couchbase.NewProcessor(
		cfg,
		connector.logger,
		connector.errorLogger,
		dcp.Commit,
	)
	if err != nil {
		return nil, err
	}

	connector.processor = processor
	if err != nil {
		return nil, err
	}

	return connector, nil
}

func newConfig(cf any) (*config.Config, error) {
	switch v := cf.(type) {
	case *config.Config:
		return v, nil
	case config.Config:
		return &v, nil
	default:
		return nil, errors.New("invalid config")
	}
}
