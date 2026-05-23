package replicator

import (
	"context"
	"log/slog"
	"time"

	"searchsurge/internal/surgecore"
	pb "searchsurge/proto"
)

type Master struct {
	pb.UnimplementedTrendServiceServer
	engine *surgecore.Engine
	logger *slog.Logger
}

func NewMaster(engine *surgecore.Engine, logger *slog.Logger) *Master {
	return &Master{engine: engine, logger: logger}
}

func (m *Master) StreamTop(_ *pb.StreamTopRequest, stream pb.TrendService_StreamTopServer) error {
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			snap := m.engine.GetSnapshotJSON()
			if len(snap) == 0 {
				continue
			}
			if err := stream.Send(&pb.TopSnapshot{
				JsonPayload:   snap,
				GeneratedAtMs: time.Now().UnixMilli(),
			}); err != nil {
				return err
			}
		}
	}
}

func (m *Master) GetTop(ctx context.Context, req *pb.GetTopRequest) (*pb.GetTopResponse, error) {
	return &pb.GetTopResponse{JsonPayload: m.engine.GetSnapshotJSON()}, nil
}

func (m *Master) UpdateStoplist(ctx context.Context, req *pb.StoplistRequest) (*pb.StoplistResponse, error) {
	m.engine.UpdateStopList(req.Words)
	return &pb.StoplistResponse{}, nil
}