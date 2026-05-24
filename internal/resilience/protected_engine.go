package resilience

import (
	"context"
	"math/rand"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"searchsurge/internal/metrics"
	"searchsurge/internal/surgecore"
)

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
	metrics.SnapshotLatencySeconds.Observe(elapsed.Seconds())
	return snap
}

func (p *ProtectedEngine) UpdateStopList(words []string) { p.core.UpdateStopList(words) }
func (p *ProtectedEngine) Run(ctx context.Context)       { p.core.Run(ctx) }
func (p *ProtectedEngine) Stop(ctx context.Context)      { p.core.Stop(ctx) }
