package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	EventsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searchsurge_events_processed_total",
			Help: "Total number of search events processed",
		},
		[]string{"status"}, // "accepted", "dropped"
	)

	IngestDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searchsurge_ingest_dropped_total",
			Help: "Total number of events dropped at ingest",
		},
		[]string{"reason"}, // "stoplist", "empty", "latency_guard", "idempotency"
	)

	SnapshotLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "searchsurge_snapshot_latency_seconds",
			Help:    "Latency of snapshot build + marshal",
			Buckets: []float64{0.001, 0.003, 0.005, 0.01, 0.02, 0.05},
		},
	)

	ActiveEntries = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "searchsurge_active_entries",
			Help: "Current number of unique queries in memory",
		},
	)

	TopSnapshotSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "searchsurge_top_snapshot_size",
			Help: "Number of items in the last rendered top snapshot",
		},
	)
)

func Register(r prometheus.Registerer) {
	r.MustRegister(EventsProcessed, IngestDropped, SnapshotLatency, ActiveEntries, TopSnapshotSize)
}