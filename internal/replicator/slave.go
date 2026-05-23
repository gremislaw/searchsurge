package replicator

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "searchsurge/proto"
)

type Slave struct {
	pb.UnimplementedTrendServiceServer
	client pb.TrendServiceClient
	snap   atomic.Pointer[[]byte]
	logger *slog.Logger
}

func NewSlave(masterAddr string, logger *slog.Logger) (*Slave, error) {
	conn, err := grpc.Dial(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Slave{
		client: pb.NewTrendServiceClient(conn),
		logger: logger,
	}, nil
}

func (s *Slave) Start(ctx context.Context) {
	go s.runStream(ctx)
}

func (s *Slave) runStream(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			stream, err := s.client.StreamTop(ctx, &pb.StreamTopRequest{})
			if err != nil {
				s.logger.Warn("stream connect failed", "err", err)
				time.Sleep(2 * time.Second)
				continue
			}
			for {
				snap, err := stream.Recv()
				if err != nil {
					s.logger.Warn("stream recv failed", "err", err)
					break
				}
				s.snap.Store(&snap.JsonPayload)
			}
		}
	}
}

func (s *Slave) GetSnapshotJSON() []byte {
	if b := s.snap.Load(); b != nil {
		return *b
	}
	return []byte("[]")
}

func (s *Slave) GetTop(ctx context.Context, req *pb.GetTopRequest) (*pb.GetTopResponse, error) {
	return &pb.GetTopResponse{JsonPayload: s.GetSnapshotJSON()}, nil
}