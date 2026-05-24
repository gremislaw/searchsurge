package replicator

import (
	"context"
	"log/slog"

	"searchsurge/internal/databus"
	"searchsurge/internal/surgecore"
)

type Master struct {
	engine surgecore.Engine
	bus    *databus.DataBus
}

func NewMaster(engine surgecore.Engine, busCfg databus.Config, logger *slog.Logger, metrics databus.MetricsObserver) *Master {
	return &Master{
		engine: engine,
		bus:    databus.New(busCfg, engine, logger, metrics),
	}
}

func (m *Master) Run(ctx context.Context) {
	m.engine.Run(ctx)
	go m.bus.Run(ctx)
}

func (m *Master) Stop(ctx context.Context) {
	m.engine.Stop(ctx)
}

func (m *Master) GetSnapshotJSON() []byte {
	return m.engine.GetSnapshotJSON()
}

func (m *Master) UpdateStopList(words []string) {
	m.engine.UpdateStopList(words)
}