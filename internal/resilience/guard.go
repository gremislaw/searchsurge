package resilience

import (
	"math/rand"
	"sync"
	"time"
)

type LatencyGuard struct {
	mu          sync.Mutex
	lastLatency time.Duration
	dropRate    float64
	threshold   time.Duration
	metrics     shared.MetricsCounter
}

func NewLatencyGuard(threshold time.Duration, metrics shared.MetricsCounter) *LatencyGuard {
	return &LatencyGuard{threshold: threshold, metrics: metrics}
}

func (g *LatencyGuard) ShouldAdmit() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.lastLatency > g.threshold {
		g.dropRate = min(1.0, g.dropRate+0.1)
		if rand.Float64() < g.dropRate {
			if g.metrics != nil {
				g.metrics.Inc()
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
	g.lastLatency = d
	g.mu.Unlock()
}
