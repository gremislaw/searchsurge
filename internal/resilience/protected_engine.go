package resilience

import (
	"context"
	"time"

	pb "searchsurge/internal/pb/proto"
	"searchsurge/internal/shared"
	"searchsurge/internal/surgecore"
)

type ProtectedEngine struct {
	core  surgecore.Engine
	guard *LatencyGuard
}

func NewProtectedEngine(core surgecore.Engine, threshold time.Duration, dropped shared.MetricsObserver) *ProtectedEngine {
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

	if p.guard.metrics != nil {
		if accepted {
			p.guard.metrics.EventProcessed(shared.LabelStatusAccepted)
		} else {
			p.guard.metrics.EventProcessed(shared.LabelStatusDropped)
		}
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

func (p *ProtectedEngine) GetSnapshotProto() *pb.GetTopResponse { return p.core.GetSnapshotProto() }
func (p *ProtectedEngine) UpdateStopList(words []string)        { p.core.UpdateStopList(words) }
func (p *ProtectedEngine) Run(ctx context.Context)              { p.core.Run(ctx) }
func (p *ProtectedEngine) Stop(ctx context.Context)             { p.core.Stop(ctx) }
