package resilience

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"searchsurge/internal/surgecore"
)

type LatencyGuard struct {
	mu          sync.Mutex
	lastLatency time.Duration
	dropRate    float64
	threshold   time.Duration
}

func NewLatencyGuard(threshold time.Duration) *LatencyGuard {
	return &LatencyGuard{threshold: threshold}
}

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

type ProtectedEngine struct {
	core  surgecore.Engine
	guard *LatencyGuard
}

func NewProtectedEngine(core surgecore.Engine, threshold time.Duration) *ProtectedEngine {
	return &ProtectedEngine{core: core, guard: NewLatencyGuard(threshold)}
}

func (p *ProtectedEngine) Ingest(query string) bool {
	if !p.guard.ShouldAdmit() {
		return false
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

func (p *ProtectedEngine) Run(ctx context.Context) { p.core.Run(ctx) }
func (p *ProtectedEngine) Stop(ctx context.Context) { p.core.Stop(ctx) }