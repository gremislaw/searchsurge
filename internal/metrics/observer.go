package metrics

import (
	"searchsurge/internal/shared"
	"time"
)

type PrometheusObserver struct{}

func NewPrometheusObserver() shared.MetricsObserver {
	return &PrometheusObserver{}
}

func (p *PrometheusObserver) EventProcessed(status string) {
	EventsProcessedTotal.WithLabelValues(status).Inc()
}

func (p *PrometheusObserver) IngestDropped(reason string) {
	IngestDroppedTotal.WithLabelValues(reason).Inc()
}

func (p *PrometheusObserver) ObserveSnapshotLatency(d time.Duration) {
	SnapshotLatencySeconds.Observe(d.Seconds())
}

func (p *PrometheusObserver) SetActiveEntries(count int) {
	ActiveEntries.Set(float64(count))
}

func (p *PrometheusObserver) SetTopSnapshotSize(count int) {
	TopSnapshotSize.Set(float64(count))
}

func (p *PrometheusObserver) SetConsumerLag(lag int64) {
	NATSConsumerLag.Set(float64(lag))
}

func (p *PrometheusObserver) SetReplicationStatus(role string, connected bool) {
	val := 0.0
	if connected {
		val = 1.0
	}
	ReplicationStreamStatus.WithLabelValues(role).Set(val)
}
