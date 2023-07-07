# Go Couchbase Connect Couchbase

[![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-couchbase-connect-couchbase.svg)](https://pkg.go.dev/github.com/Trendyol/go-couchbase-connect-couchbase) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-couchbase-connect-couchbase)](https://goreportcard.com/report/github.com/Trendyol/go-couchbase-connect-couchbase)

**Go Couchbase Connect Couchbase** streams documents from Couchbase Database Change Protocol (DCP) and writes to
Couchbase bucket in near real-time.

## Features

* **Less resource usage** and **higher throughput**.
* **Update multiple documents** for a DCP event(see [Example](#example)).
* Handling different DCP events such as **expiration, deletion and mutation**(see [Example](#example)).
* **Managing batch configurations** such as maximum batch size, batch bytes, batch ticker durations.
* **Scale up and down** by custom membership algorithms(Couchbase, KubernetesHa, Kubernetes StatefulSet or
  Static, see [examples](https://github.com/Trendyol/go-dcp-client#examples)).
* **Easily manageable configurations**.

## Example
[Struct Config](example/struct-config/main.go)
```go
func defaultMapper(event couchbase.Event) []couchbase.CBActionDocument {
    if event.IsMutated {
        return []couchbase.CBActionDocument{couchbase.NewIndexAction(event.Key, event.Value)}
    }
    return []couchbase.CBActionDocument{couchbase.NewDeleteAction(event.Key)}
}

func main() {
    c, err := gocouchbaseconnectcouchbase.NewConnector(&config.Config{
        Dcp: dcpClientConfig.Dcp{
            Hosts:      []string{"localhost:8091"},
            Username:   "user",
            Password:   "password",
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
                    "bucket":     "checkpoint-bucket-name",
                    "scope":      "_default",
                    "collection": "_default",
                },
                Type: "couchbase",
            },
            Debug: true
        },
        Couchbase: config.Couchbase{
            Hosts:            []string{"localhost:8091"},
            Username:         "user",
            Password:         "password",
            BucketName:       "dcp-test-backup",
            BatchSizeLimit:   10,
            RequestTimeoutMs: 1000 * 10,
        },
    }, defaultMapper, logger.Log, logger.ErrorLog)

    if err != nil {
        panic(err)
    }
    
    defer c.Close()
    c.Start()
}

```

## Configuration

### Dcp Configuration

Check out on [go-dcp-client](https://github.com/Trendyol/go-dcp-client#configuration)

### Couchbase Specific Configuration

| Variable                           | Type          | Required | Default  | Description                                                                                         |                                                           
|------------------------------------|---------------|----------|----------|-----------------------------------------------------------------------------------------------------|
| `Hosts`                            | []string      | yes      |          | Couchbase connection urls                                                                           |
| `BucketName`                       | string        | yes      |          | Defines Couchbase bucket name                                                                       |
| `ScopeName`                        | string        | no       | _default | Defines Couchbase scope name                                                                        |
| `CollectionName`                   | string        | no       | _default | Defines Couchbase collection name                                                                   |
| `BatchSizeLimit`                   | int           | no       | 1000     | Maximum message count for batch, if exceed flush will be triggered.                                 |
| `BatchTickerDuration`              | time.Duration | no       | 10s      | Batch is being flushed automatically at specific time intervals for long waiting messages in batch. |
| `BatchByteSizeLimit`               | int           | no       | 10485760 | Maximum size(byte) for batch, if exceed flush will be triggered.                                    |
| `RequestTimeoutMs`                 | int           | no       | 3000     | Maximum request waiting time. Value type milliseconds.                                              |

## Exposed metrics

For DCP related metrics see [also](https://github.com/Trendyol/go-dcp-client#exposed-metrics).

## Contributing

Go Couchbase Connect Couchbase is always open for direct contributions. For more information please check
our [Contribution Guideline document](./CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).