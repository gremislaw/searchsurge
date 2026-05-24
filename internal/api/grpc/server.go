package grpc

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"searchsurge/internal/api"
	pb "searchsurge/internal/pb/proto"
)

const (
	MAXLIMIT     = 2000
	DEFAULTLIMIT = 5
)

type Server struct {
	pb.UnimplementedTrendServiceServer
	provider api.TrendProvider
}

func NewServer(provider api.TrendProvider) *Server {
	return &Server{provider: provider}
}

func (s *Server) Register(grpcSrv *grpc.Server) { pb.RegisterTrendServiceServer(grpcSrv, s) }

func (s *Server) GetTop(ctx context.Context, req *pb.GetTopRequest) (*pb.GetTopResponse, error) {
	rawJSON := s.provider.GetSnapshotJSON()
	if string(rawJSON) == "[]" {
		return &pb.GetTopResponse{}, nil
	}

	var items []*pb.TrendItem
	if err := json.Unmarshal(rawJSON, &items); err != nil {
		return nil, status.Errorf(codes.Internal, "snapshot parse error: %v", err)
	}

	limit := int(req.GetN())
	if limit <= 0 {
		limit = DEFAULTLIMIT
	}
	if limit > maxLimit {
		limit = MAXLIMIT
	}

	if len(items) > limit {
		items = items[:limit]
	}

	return &pb.GetTopResponse{Items: items}, nil
}

func (s *Server) UpdateStoplist(ctx context.Context, req *pb.StoplistRequest) (*pb.StoplistResponse, error) {
	if len(req.Words) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty words")
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
			if err := stream.Send(&pb.TopSnapshot{JsonPayload: snap, GeneratedAtMs: time.Now().UnixMilli()}); err != nil {
				return status.Errorf(codes.Unavailable, "stream failed: %v", err)
			}
		}
	}
}
