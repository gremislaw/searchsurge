package resilience

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"searchsurge/internal/metrics"
	"searchsurge/internal/surgecore"
)

func NewLatencyGuard(threshold time.Duration, dropped prometheus.Counter) *LatencyGuard {
	return &LatencyGuard{threshold: threshold, dropped: dropped}
}

func (g *LatencyGuard) ShouldAdmit() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.lastLatency > g.threshold {
		g.dropRate = min(1.0, g.dropRate+0.1)
		if rand.Float64() < g.dropRate {
			if g.dropped != nil {
				g.dropped.Inc()
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

type ProtectedEngine struct {
	core  surgecore.Engine
	guard *LatencyGuard
}

func NewProtectedEngine(core surgecore.Engine, threshold time.Duration, dropped prometheus.Counter) *ProtectedEngine {
	return &ProtectedEngine{
		core:  core,
		guard: NewLatencyGuard(threshold, dropped),
	}
}

func (p *ProtectedEngine) Ingest(query string) bool {
	admitted := p.guard.ShouldAdmit()
	if !admitted {
		return false
	}
	accepted := p.core.Ingest(query)
	if accepted {
		metrics.EventsProcessed.WithLabelValues("accepted").Inc()
	} else {
		metrics.EventsProcessed.WithLabelValues("dropped").Inc()
	}
	return accepted
}

func (p *ProtectedEngine) GetSnapshotJSON() []byte {
	start := time.Now()
	snap := p.core.GetSnapshotJSON()
	elapsed := time.Since(start)
	p.guard.RecordLatency(elapsed)
	metrics.SnapshotLatency.Observe(elapsed.Seconds())
	return snap
}

func (p *ProtectedEngine) UpdateStopList(words []string) { p.core.UpdateStopList(words) }
func (p *ProtectedEngine) Run(ctx context.Context)       { p.core.Run(ctx) }
func (p *ProtectedEngine) Stop(ctx context.Context)      { p.core.Stop(ctx) }