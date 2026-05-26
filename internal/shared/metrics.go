package shared

import "time"

type MetricsCounter interface {
	Inc()
	Add(float64)
}

type MetricsObserver interface {
	EventProcessed(status string)
	IngestDropped(reason string)
	ObserveSnapshotLatency(d time.Duration)
	SetActiveEntries(count int)
	SetTopSnapshotSize(count int)
	SetConsumerLag(lag int64)
	SetReplicationStatus(role string, connected bool)
}

type NoopMetricsObserver struct{}

func (n NoopMetricsObserver) EventProcessed(status string)             {}
func (n NoopMetricsObserver) IngestDropped(reason string)              {}
func (n NoopMetricsObserver) ObserveSnapshotLatency(d time.Duration)   {}
func (n NoopMetricsObserver) SetActiveEntries(count int)               {}
func (n NoopMetricsObserver) SetTopSnapshotSize(count int)             {}
func (n NoopMetricsObserver) SetConsumerLag(lag int64)                 {}
func (n NoopMetricsObserver) SetReplicationStatus(role string, c bool) {}
