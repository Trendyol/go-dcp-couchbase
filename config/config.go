package config

import (
	"time"

	"github.com/Trendyol/go-dcp/config"
)

const (
	DefaultScopeName      = "_default"
	DefaultCollectionName = "_default"
)

type Couchbase struct {
	RootCAPath           string        `yaml:"rootCAPath"`
	Password             string        `yaml:"password"`
	BucketName           string        `yaml:"bucketName"`
	ScopeName            string        `yaml:"scopeName"`
	CollectionName       string        `yaml:"collectionName"`
	Username             string        `yaml:"username"`
	Hosts                []string      `yaml:"hosts"`
	ConnectionBufferSize uint          `yaml:"connectionBufferSize"`
	BatchTickerDuration  time.Duration `yaml:"batchTickerDuration"`
	ConnectionTimeout    time.Duration `yaml:"connectionTimeout"`
	BatchSizeLimit       int           `yaml:"batchSizeLimit"`
	BatchByteSizeLimit   int           `yaml:"batchByteSizeLimit"`
	RequestTimeout       time.Duration `yaml:"requestTimeout"`
	SecureConnection     bool          `yaml:"secureConnection"`
}

type Config struct {
	Couchbase Couchbase  `yaml:"couchbase"`
	Dcp       config.Dcp `yaml:",inline"`
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
}

func (c *Config) applyDefaultProcess() {
	if c.Couchbase.BatchTickerDuration == 0 {
		c.Couchbase.BatchTickerDuration = 10 * time.Second
	}

	if c.Couchbase.BatchSizeLimit == 0 {
		c.Couchbase.BatchSizeLimit = 1000
	}

	if c.Couchbase.BatchByteSizeLimit == 0 {
		c.Couchbase.BatchByteSizeLimit = 10485760
	}

	if c.Couchbase.RequestTimeout == 0 {
		c.Couchbase.RequestTimeout = 3 * time.Second
	}
}
