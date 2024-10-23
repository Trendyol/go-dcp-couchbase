package integration

import (
	"context"
	"github.com/Trendyol/go-dcp"
	dcpcouchbase "github.com/Trendyol/go-dcp-couchbase"
	"github.com/Trendyol/go-dcp/models"
	"sync"
	"testing"
	"time"
)

func TestCouchbase(t *testing.T) {
	time.Sleep(time.Minute)

	connector, err := dcpcouchbase.NewConnectorBuilder("config.yml").
		SetMapper(dcpcouchbase.DefaultMapper).
		Build()
	if err != nil {
		t.Fatal(err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		connector.Start()
	}()

	go func() {
		finish := make(chan struct{}, 1)

		totalEvent := 0
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Minute)

		dcp, err := dcp.NewDcp("config_dcp.yml", func(e *models.ListenerContext) {
			select {
			case <-ctx.Done():
				t.Fatalf("deadline exceed")
			default:
				totalEvent += 1

				if totalEvent == 31591 {
					finish <- struct{}{}
					break
				}
			}

			e.Ack()
		})

		if err != nil {
			panic(err)
		}

		go func() {
			<-finish
			dcp.Close()
			connector.Close()
		}()

		dcp.Start()
	}()

	wg.Wait()
}

type CountResponse struct {
	Count int64 `json:"count"`
}
