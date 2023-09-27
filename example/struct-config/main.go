package main

import (
	"github.com/Trendyol/go-dcp-couchbase"
	"time"

	"github.com/Trendyol/go-dcp-couchbase/config"
	dcpConfig "github.com/Trendyol/go-dcp/config"
)

func main() {
	c, err := dcpcouchbase.NewConnector(&config.Config{
		Dcp: dcpConfig.Dcp{
			Hosts:      []string{"localhost:8091"},
			Username:   "user",
			Password:   "password",
			BucketName: "dcp-test",
			Dcp: dcpConfig.ExternalDcp{
				Group: dcpConfig.DCPGroup{
					Name: "groupName",
					Membership: dcpConfig.DCPGroupMembership{
						RebalanceDelay: 3 * time.Second,
					},
				},
			},
			Metadata: dcpConfig.Metadata{
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
			Hosts:          []string{"localhost:8091"},
			Username:       "user",
			Password:       "password",
			BucketName:     "dcp-test-backup",
			BatchSizeLimit: 10,
			RequestTimeout: 10 * time.Second,
		},
	}, dcpcouchbase.DefaultMapper)
	if err != nil {
		panic(err)
	}

	defer c.Close()
	c.Start()
}
