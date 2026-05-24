package grpc

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "searchsurge/internal/pb"
	"searchsurge/internal/service"
)

type Server struct {
	pb.UnimplementedTrendServiceServer
	provider service.Provider
	logger   *slog.Logger
}

func NewServer(provider service.Provider, logger *slog.Logger) *Server {
	return &Server{provider: provider, logger: logger}
}

func (s *Server) Register(grpcSrv *grpc.Server) {
	pb.RegisterTrendServiceServer(grpcSrv, s)
}

func (s *Server) GetTop(ctx context.Context, req *pb.GetTopRequest) (*pb.GetTopResponse, error) {
	return &pb.GetTopResponse{JsonPayload: s.provider.GetSnapshotJSON()}, nil
}

func (s *Server) UpdateStoplist(ctx context.Context, req *pb.StoplistRequest) (*pb.StoplistResponse, error) {
	if len(req.Words) == 0 {
		return nil, status.Error(codes.InvalidArgument, "words list cannot be empty")
	}
	s.provider.UpdateStopList(req.Words)
	return &pb.StoplistResponse{}, nil
}

func (s *Server) StreamTop(req *pb.StreamTopRequest, stream pb.TrendService_StreamTopServer) error {
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			snap := s.provider.GetSnapshotJSON()
			if len(snap) == 0 || string(snap) == "[]" {
				continue
			}
			if err := stream.Send(&pb.TopSnapshot{
				JsonPayload:   snap,
				GeneratedAtMs: time.Now().UnixMilli(),
			}); err != nil {
				return status.Errorf(codes.Unavailable, "stream send failed: %v", err)
			}
		}
	}
}