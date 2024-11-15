package metric

import (
	"github.com/Trendyol/go-dcp-couchbase/couchbase"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	processor                 *couchbase.Processor
	getMapperProcessLatencyMs func() int64

	processLatency            *prometheus.Desc
	mapperProcessLatency      *prometheus.Desc
	bulkRequestProcessLatency *prometheus.Desc
	bulkRequestSize           *prometheus.Desc
	bulkRequestByteSize       *prometheus.Desc
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

	ch <- prometheus.MustNewConstMetric(
		s.bulkRequestProcessLatency,
		prometheus.GaugeValue,
		float64(processorMetric.BulkRequestProcessLatencyMs),
		[]string{}...,
	)
	ch <- prometheus.MustNewConstMetric(
		s.bulkRequestSize,
		prometheus.GaugeValue,
		float64(processorMetric.BulkRequestSize),
		[]string{}...,
	)
	ch <- prometheus.MustNewConstMetric(
		s.bulkRequestByteSize,
		prometheus.GaugeValue,
		float64(processorMetric.BulkRequestByteSize),
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

		bulkRequestProcessLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "couchbase_connector_bulk_request_process_latency_ms", "current"),
			"Couchbase connector bulk request process latency ms",
			[]string{},
			nil,
		),
		bulkRequestSize: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "couchbase_connector_bulk_request_size", "current"),
			"Couchbase connector bulk request size",
			[]string{},
			nil,
		),
		bulkRequestByteSize: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "couchbase_connector_bulk_request_byte_size", "current"),
			"Couchbase connector bulk request byte size",
			[]string{},
			nil,
		),
	}
}
