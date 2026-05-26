package replicator

import (
	"context"

	"searchsurge/internal/infrastructure/databus"
	"searchsurge/internal/shared"
	"searchsurge/internal/surgecore"
)

type Master struct {
	engine surgecore.Engine
	bus    *databus.DataBus
	logger shared.Logger
}

func NewMaster(engine surgecore.Engine, busCfg databus.Config, logger shared.Logger, metrics shared.MetricsObserver) *Master {
	return &Master{
		engine: engine,
		bus:    databus.New(busCfg, engine, logger, metrics),
		logger: logger,
	}
}

func (m *Master) Run(ctx context.Context) {
	m.engine.Run(ctx)
	go func() {
		if err := m.bus.Run(ctx); err != nil {
			m.logger.Error("databus consumer failed", "err", err)
		}
	}()
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
