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
	Password             string        `yaml:"password"`
	BucketName           string        `yaml:"bucketName"`
	ScopeName            string        `yaml:"scopeName"`
	CollectionName       string        `yaml:"collectionName"`
	Username             string        `yaml:"username"`
	RootCAPath           string        `yaml:"rootCAPath"`
	Hosts                []string      `yaml:"hosts"`
	BatchTickerDuration  time.Duration `yaml:"batchTickerDuration"`
	ConnectionTimeout    time.Duration `yaml:"connectionTimeout"`
	BatchSizeLimit       int           `yaml:"batchSizeLimit"`
	MaxQueueSize         int           `yaml:"maxQueueSize"`
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
	c.Couchbase.ConnectionTimeout = 5 * time.Second
	c.Couchbase.ConnectionBufferSize = 20971520

	if c.Couchbase.MaxQueueSize == 0 {
		c.Couchbase.MaxQueueSize = 2048
	}
}

func (c *Config) applyDefaultProcess() {
	if c.Couchbase.BatchTickerDuration == 0 {
		c.Couchbase.BatchTickerDuration = 10 * time.Second
	}

	if c.Couchbase.BatchSizeLimit == 0 {
		c.Couchbase.BatchSizeLimit = 1000
	}

	if c.Couchbase.BatchByteSizeLimit == nil {
		c.Couchbase.BatchByteSizeLimit = helpers.ResolveUnionIntOrStringValue("10mb")
	}

	if c.Couchbase.RequestTimeout == 0 {
		c.Couchbase.RequestTimeout = 3 * time.Second
	}
}
