package resilience

import (
	"time"
	"searchsurge/internal/surgecore"
)

type ProtectedEngine struct {
	core  *surgecore.Engine
	guard *LatencyGuard
}

func NewProtectedEngine(core *surgecore.Engine, threshold time.Duration) *ProtectedEngine {
	return &ProtectedEngine{
		core:  core,
		guard: NewLatencyGuard(threshold),
	}
}

func (p *ProtectedEngine) Ingest(query string) bool {
	if !p.guard.ShouldAdmit() {
		return false // метрика: events_dropped{reason="latency_guard"}
	}
	return p.core.Ingest(query)
}

func (p *ProtectedEngine) GetSnapshotJSON() []byte {
	start := time.Now()
	snap := p.core.GetSnapshotJSON()
	p.guard.RecordLatency(time.Since(start))
	return snap
}

func (p *ProtectedEngine) UpdateStopList(words []string) {
	p.core.UpdateStopList(words)
}