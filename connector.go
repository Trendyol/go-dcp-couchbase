package dcpcouchbase

import (
	"errors"
	"os"

	"github.com/Trendyol/go-dcp"

	"gopkg.in/yaml.v3"

	"github.com/sirupsen/logrus"

	"github.com/Trendyol/go-dcp-couchbase/config"
	"github.com/Trendyol/go-dcp-couchbase/couchbase"
	"github.com/Trendyol/go-dcp-couchbase/metric"

	dcpClientConfig "github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
)

type Connector interface {
	Start()
	Close()
}

type connector struct {
	dcp          dcp.Dcp
	config       *config.Config
	mapper       Mapper
	processor    *couchbase.Processor
	targetClient couchbase.TargetClient
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
		e = couchbase.NewMutateEvent(event.Key, event.Value, event.CollectionName, couchbase.NewMutateMetadata(event))
	case models.DcpExpiration:
		e = couchbase.NewExpireEvent(event.Key, nil, event.CollectionName, couchbase.NewExpireMetadata(event))
	case models.DcpDeletion:
		e = couchbase.NewDeleteEvent(event.Key, nil, event.CollectionName, couchbase.NewDeleteMetadata(event))
	default:
		return
	}

	actions := c.mapper(
		couchbase.EventContext{
			TargetClient: c.targetClient,
			Event:        e,
		},
	)

	if len(actions) == 0 {
		ctx.Ack()
		return
	}

	c.processor.AddActions(ctx, e.Metadata.EventTime, actions)
}

func createDcp(cfg any, listener models.Listener) (dcp.Dcp, error) {
	switch v := cfg.(type) {
	case dcpClientConfig.Dcp:
		return dcp.NewDcp(v, listener)
	case string:
		return dcp.NewDcp(v, listener)
	default:
		return nil, errors.New("invalid config")
	}
}

func newConnector(cf any, mapper Mapper) (Connector, error) {
	cfg, err := newConfig(cf)
	if err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()

	connector := &connector{
		mapper: mapper,
		config: cfg,
	}

	if err != nil {
		return nil, err
	}

	dcp, err := createDcp(cfg.Dcp, connector.listener)
	if err != nil {
		logger.Log.Error("dcp error: %v", err)
		return nil, err
	}

	dcpConfig := dcp.GetConfig()
	dcpConfig.Checkpoint.Type = "manual"

	connector.dcp = dcp

	client := couchbase.NewClient(&cfg.Couchbase)
	err = client.Connect()
	if err != nil {
		return nil, err
	}

	connector.targetClient = couchbase.NewTargetClient(cfg, client)

	processor, err := couchbase.NewProcessor(
		cfg,
		client,
		dcp.Commit,
	)
	if err != nil {
		return nil, err
	}

	connector.processor = processor
	if err != nil {
		return nil, err
	}
	connector.dcp.SetEventHandler(
		&DcpEventHandler{
			processor: connector.processor,
		})

	metricCollector := metric.NewMetricCollector(connector.processor)
	dcp.SetMetricCollectors(metricCollector)

	return connector, nil
}

func newConfig(cf any) (*config.Config, error) {
	switch v := cf.(type) {
	case *config.Config:
		return v, nil
	case config.Config:
		return &v, nil
	case string:
		return newConnectorConfigFromPath(v)
	default:
		return nil, errors.New("invalid config")
	}
}

func newConnectorConfigFromPath(path string) (*config.Config, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c config.Config
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

type ConnectorBuilder struct {
	mapper Mapper
	config any
}

func NewConnectorBuilder(config any) *ConnectorBuilder {
	return &ConnectorBuilder{
		config: config,
		mapper: DefaultMapper,
	}
}

func (c *ConnectorBuilder) SetMapper(mapper Mapper) *ConnectorBuilder {
	c.mapper = mapper
	return c
}

func (c *ConnectorBuilder) Build() (Connector, error) {
	return newConnector(c.config, c.mapper)
}

func (c *ConnectorBuilder) SetLogger(l *logrus.Logger) *ConnectorBuilder {
	logger.Log = &logger.Loggers{
		Logrus: l,
	}
	return c
}
