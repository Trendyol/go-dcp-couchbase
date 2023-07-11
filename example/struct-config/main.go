package main

import (
	"github.com/Trendyol/go-dcp-couchbase"
	"time"

	dcpClientConfig "github.com/Trendyol/go-dcp-client/config"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-couchbase/config"
)

func main() {
	c, err := dcpcouchbase.NewConnector(&config.Config{
		Dcp: dcpClientConfig.Dcp{
			Hosts:      []string{"localhost:8091"},
			Username:   "user",
			Password:   "123456",
			BucketName: "dcp-test",
			Dcp: dcpClientConfig.ExternalDcp{
				Group: dcpClientConfig.DCPGroup{
					Name: "groupName",
					Membership: dcpClientConfig.DCPGroupMembership{
						RebalanceDelay: 3 * time.Second,
					},
				},
			},
			Metadata: dcpClientConfig.Metadata{
				Config: map[string]string{
					"bucket":     "dcp-test-meta",
					"scope":      "_default",
					"collection": "_default",
				},
				Type: "couchbase",
			},
			Debug: true,
		},
		Couchbase: config.Couchbase{
			Hosts:            []string{"localhost:8091"},
			Username:         "user",
			Password:         "123456",
			BucketName:       "dcp-test-backup",
			BatchSizeLimit:   10,
			RequestTimeoutMs: 1000 * 10,
		},
	}, dcpcouchbase.DefaultMapper, logger.Log, logger.ErrorLog)
	if err != nil {
		panic(err)
	}

	defer c.Close()
	c.Start()
}
