package dcpcouchbase

import (
	"github.com/Trendyol/go-dcp-couchbase/couchbase"
)

type DcpEventHandler struct {
	processor *couchbase.Processor
	isFinite  bool
}

func (d *DcpEventHandler) BeforeRebalanceStart() {
}

func (d *DcpEventHandler) AfterRebalanceStart() {
}

func (d *DcpEventHandler) BeforeRebalanceEnd() {
}

func (d *DcpEventHandler) AfterRebalanceEnd() {
}

func (d *DcpEventHandler) BeforeStreamStart() {
	d.processor.PrepareEndRebalancing()
}

func (d *DcpEventHandler) AfterStreamStart() {
}

func (d *DcpEventHandler) BeforeStreamStop() {
	if d.isFinite {
		return
	}
	d.processor.PrepareStartRebalancing()
}

func (d *DcpEventHandler) AfterStreamStop() {
}
