package metric

import (
	"github.com/Trendyol/go-dcp-couchbase/couchbase"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	processor                 *couchbase.Processor
	getMapperProcessLatencyMs func() int64

	processLatency       *prometheus.Desc
	mapperProcessLatency *prometheus.Desc
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
		s.mapperProcessLatency,
		prometheus.GaugeValue,
		float64(s.getMapperProcessLatencyMs()),
		[]string{}...,
	)
}

func NewMetricCollector(processor *couchbase.Processor, getMapperProcessLatencyMs func() int64) *Collector {
	return &Collector{
		processor:                 processor,
		getMapperProcessLatencyMs: getMapperProcessLatencyMs,

		processLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "couchbase_connector_latency_ms", "current"),
			"Couchbase connector latency ms",
			[]string{},
			nil,
		),

		mapperProcessLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "couchbase_connector_mapper_latency_ms", "current"),
			"Couchbase connector mapper latency ms",
			[]string{},
			nil,
		),
	}
}
