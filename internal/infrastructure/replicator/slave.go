package replicator

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "searchsurge/internal/pb/proto"
)

type Slave struct {
	masterAddr string
	client     pb.TrendServiceClient
	snap       atomic.Pointer[[]byte]
	logger     *slog.Logger
}

func NewSlave(masterAddr string, logger *slog.Logger) *Slave {
	return &Slave{masterAddr: masterAddr, logger: logger}
}

func (s *Slave) Run(ctx context.Context) {
	go s.stream(ctx)
}

func (s *Slave) stream(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := grpc.DialContext(ctx, s.masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}
			s.client = pb.NewTrendServiceClient(conn)
			stream, err := s.client.StreamTop(ctx, &pb.StreamTopRequest{})
			if err != nil {
				conn.Close()
				time.Sleep(2 * time.Second)
				continue
			}
			s.consume(ctx, stream, conn)
		}
	}
}

func (s *Slave) consume(ctx context.Context, stream pb.TrendService_StreamTopClient, conn *grpc.ClientConn) {
	defer conn.Close()
	for {
		snap, err := stream.Recv()
		if err != nil {
			return
		}
		s.snap.Store(&snap.JsonPayload)
	}
}

func (s *Slave) Stop(ctx context.Context) {}

func (s *Slave) GetSnapshotJSON() []byte {
	if b := s.snap.Load(); b != nil {
		return *b
	}

	return []byte("[]")
}

func (s *Slave) UpdateStopList([]string) {}
