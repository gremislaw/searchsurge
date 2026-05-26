package resilience

import (
	"math/rand"
	"sync"
	"time"

	"searchsurge/internal/shared"
)

type LatencyGuard struct {
	mu          sync.Mutex
	lastLatency time.Duration
	dropRate    float64
	threshold   time.Duration
	metrics     shared.MetricsObserver
}

func NewLatencyGuard(threshold time.Duration, metrics shared.MetricsObserver) *LatencyGuard {
	return &LatencyGuard{threshold: threshold, metrics: metrics}
}

func (g *LatencyGuard) ShouldAdmit() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.lastLatency > g.threshold {
		g.dropRate = min(1.0, g.dropRate+0.1)
		if rand.Float64() < g.dropRate {
			if g.metrics != nil {
				g.metrics.IngestDropped("latency_guard")
			}
			return false
		}
		return true
	}
	if g.lastLatency < g.threshold/2 {
		g.dropRate = max(0.0, g.dropRate-0.05)
	}
	return true
}

func (g *LatencyGuard) RecordLatency(d time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lastLatency = d
	if g.metrics != nil {
		g.metrics.ObserveSnapshotLatency(d)
	}
}
