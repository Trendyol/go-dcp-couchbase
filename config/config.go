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
	Username             string        `yaml:"username"`
	Password             string        `yaml:"password"`
	BucketName           string        `yaml:"bucketName"`
	ScopeName            string        `yaml:"scopeName"`
	CollectionName       string        `yaml:"collectionName"`
	RootCAPath           string        `yaml:"rootCAPath"`
	Hosts                []string      `yaml:"hosts"`
	WritePoolSizePerNode int           `yaml:"writePoolSizePerNode"`
	MaxInflightRequests  int           `yaml:"maxInflightRequests"`
	ConnectionTimeout    time.Duration `yaml:"connectionTimeout"`
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
	if c.Couchbase.MaxInflightRequests == 0 {
		c.Couchbase.MaxInflightRequests = 2048
	}

	if c.Couchbase.WritePoolSizePerNode == 0 {
		c.Couchbase.WritePoolSizePerNode = 1
	}

	if c.Couchbase.RequestTimeout == 0 {
		c.Couchbase.RequestTimeout = 3 * time.Second
	}
}
