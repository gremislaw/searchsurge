package resilience

import (
	"context"
	"time"

	"searchsurge/internal/shared"
	"searchsurge/internal/surgecore"
)

type ProtectedEngine struct {
	core  surgecore.Engine
	guard *LatencyGuard
}

func NewProtectedEngine(core surgecore.Engine, threshold time.Duration, dropped shared.MetricsCounter) *ProtectedEngine {
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
		p.guard.metrics.EventProcessedTotal("accepted")
	} else {
		p.guard.metrics.EventProcessedTotal("dropped")
	}
	return accepted
}

func (p *ProtectedEngine) GetSnapshotJSON() []byte {
	start := time.Now()
	snap := p.core.GetSnapshotJSON()
	elapsed := time.Since(start)
	p.guard.RecordLatency(elapsed)
	return snap
}

func (p *ProtectedEngine) UpdateStopList(words []string) { p.core.UpdateStopList(words) }
func (p *ProtectedEngine) Run(ctx context.Context)       { p.core.Run(ctx) }
func (p *ProtectedEngine) Stop(ctx context.Context)      { p.core.Stop(ctx) }
