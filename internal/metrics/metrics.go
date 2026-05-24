package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	EventsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "searchsurge_events_processed_total", Help: "Total events handled"},
		[]string{"status"}, // accepted, dropped
	)
	IngestDroppedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "searchsurge_ingest_dropped_total", Help: "Events dropped at ingest"},
		[]string{"reason"}, // stoplist, empty, latency_guard, idempotency
	)

	SnapshotLatencySeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "searchsurge_snapshot_latency_seconds",
			Help:    "Build + marshal latency",
			Buckets: []float64{0.001, 0.003, 0.005, 0.01, 0.02, 0.05},
		},
	)

	ActiveEntries = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "searchsurge_active_entries", Help: "Unique queries in memory"},
	)
	TopSnapshotSize = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "searchsurge_top_snapshot_size", Help: "Items in last rendered snapshot"},
	)

	NATSConsumerLag = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "searchsurge_nats_consumer_lag", Help: "Pending messages in stream"},
	)
	ReplicationStreamStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "searchsurge_replication_stream_status", Help: "1=connected, 0=disconnected"},
		[]string{"role"},
	)
)

func Register(r prometheus.Registerer) {
	r.MustRegister(
		EventsProcessedTotal, IngestDroppedTotal, SnapshotLatencySeconds,
		ActiveEntries, TopSnapshotSize, NATSConsumerLag, ReplicationStreamStatus,
	)
}