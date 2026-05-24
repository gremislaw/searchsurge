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
	dropped     prometheus.Counter
}

func NewLatencyGuard(threshold time.Duration) *LatencyGuard {
	return &LatencyGuard{threshold: threshold}
}

// При превышении latency растёт dropRate, часть запросов дропается.
func (g *LatencyGuard) ShouldAdmit() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.lastLatency > g.threshold {
		g.dropRate = min(1.0, g.dropRate+0.1)
		return rand.Float64() > g.dropRate
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
