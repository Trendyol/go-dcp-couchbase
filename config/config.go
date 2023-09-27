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
	RootCAPath           string
	Password             string
	BucketName           string
	ScopeName            string
	CollectionName       string
	Username             string
	Hosts                []string
	ConnectionBufferSize uint
	BatchTickerDuration  time.Duration
	ConnectionTimeout    time.Duration
	BatchSizeLimit       int
	BatchByteSizeLimit   int
	RequestTimeout       time.Duration
	SecureConnection     bool
}

type Config struct {
	Couchbase Couchbase
	Dcp       config.Dcp
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
