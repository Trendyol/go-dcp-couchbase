package metric

import (
	"github.com/Trendyol/go-dcp-couchbase/couchbase"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	processor *couchbase.Processor

	processLatency            *prometheus.Desc
	bulkRequestProcessLatency *prometheus.Desc
}

func (s *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

func (s *Collector) Collect(ch chan<- prometheus.Metric) {
	processorMetric := s.processor.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		s.processLatency,
		prometheus.GaugeValue,
		float64(processorMetric.ProcessLatencyMs),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.bulkRequestProcessLatency,
		prometheus.GaugeValue,
		float64(processorMetric.BulkRequestProcessLatencyMs),
		[]string{}...,
	)
}

func NewMetricCollector(processor *couchbase.Processor) *Collector {
	return &Collector{
		processor: processor,

		processLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "couchbase_connector_latency_ms", "current"),
			"Couchbase connector latency ms",
			[]string{},
			nil,
		),

		bulkRequestProcessLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "couchbase_connector_bulk_request_process_latency_ms", "current"),
			"Couchbase connector bulk request process latency ms",
			[]string{},
			nil,
		),
	}
}
