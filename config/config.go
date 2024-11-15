package config

import (
	"time"

	"github.com/Trendyol/go-dcp/helpers"

	"github.com/Trendyol/go-dcp/config"
)

const (
	DefaultScopeName      = "_default"
	DefaultCollectionName = "_default"
)

type Couchbase struct {
	BatchByteSizeLimit   any           `yaml:"batchByteSizeLimit"`
	RootCAPath           string        `yaml:"rootCAPath"`
	CollectionName       string        `yaml:"collectionName"`
	Username             string        `yaml:"username"`
	Password             string        `yaml:"password"`
	BucketName           string        `yaml:"bucketName"`
	ScopeName            string        `yaml:"scopeName"`
	Hosts                []string      `yaml:"hosts"`
	BatchSizeLimit       int           `yaml:"batchSizeLimit"`
	BatchTickerDuration  time.Duration `yaml:"batchTickerDuration"`
	WritePoolSizePerNode int           `yaml:"writePoolSizePerNode"`
	MaxInflightRequests  int           `yaml:"maxInflightRequests"`
	ConnectionTimeout    time.Duration `yaml:"connectionTimeout"`
	ConnectionBufferSize uint          `yaml:"connectionBufferSize"`
	RequestTimeout       time.Duration `yaml:"requestTimeout"`
	SecureConnection     bool          `yaml:"secureConnection"`
}

type Config struct {
	Couchbase Couchbase  `yaml:"couchbase" mapstructure:"couchbase"`
	Dcp       config.Dcp `yaml:",inline" mapstructure:",squash"`
}

func (c *Config) ApplyDefaults() {
	c.applyDefaultScopeName()
	c.applyDefaultCollections()
	c.applyDefaultConnectionSettings()
	c.applyDefaultProcess()
}

func (c *Config) applyDefaultCollections() {
	if c.Couchbase.CollectionName == "" {
		c.Couchbase.CollectionName = DefaultCollectionName
	}
}

func (c *Config) applyDefaultScopeName() {
	if c.Couchbase.ScopeName == "" {
		c.Couchbase.ScopeName = DefaultScopeName
	}
}

func (c *Config) applyDefaultConnectionSettings() {
	c.Couchbase.ConnectionTimeout = 1 * time.Minute
	c.Couchbase.ConnectionBufferSize = 20971520
}

func (c *Config) applyDefaultProcess() {
	if c.Couchbase.WritePoolSizePerNode == 0 {
		c.Couchbase.WritePoolSizePerNode = 1
	}

	if c.Couchbase.BatchTickerDuration == 0 {
		c.Couchbase.BatchTickerDuration = 10 * time.Second
	}

	if c.Couchbase.BatchSizeLimit == 0 {
		c.Couchbase.BatchSizeLimit = 2048
	}

	if c.Couchbase.MaxInflightRequests == 0 {
		c.Couchbase.MaxInflightRequests = c.Couchbase.BatchSizeLimit
	}

	if c.Couchbase.BatchByteSizeLimit == nil {
		c.Couchbase.BatchByteSizeLimit = helpers.ResolveUnionIntOrStringValue("10mb")
	}

	if c.Couchbase.RequestTimeout == 0 {
		c.Couchbase.RequestTimeout = 1 * time.Minute
	}
}
