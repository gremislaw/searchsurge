package replicator

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "searchsurge/internal/pb/proto"
	"searchsurge/internal/shared"
)

type Slave struct {
	masterAddr string
	client     pb.TrendServiceClient
	snap       atomic.Pointer[[]byte]
	logger     shared.Logger
}

func NewSlave(masterAddr string, logger shared.Logger) *Slave {
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
			conn, err := grpc.NewClient(s.masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				s.logger.Error("grpc dial failed", "err", err)
				time.Sleep(2 * time.Second)
				continue
			}
			s.client = pb.NewTrendServiceClient(conn)
			stream, err := s.client.StreamTop(ctx, &pb.StreamTopRequest{})
			if err != nil {
				if err := conn.Close(); err != nil {
					s.logger.Debug("grpc conn close error", "err", err)
				}
				time.Sleep(2 * time.Second)
				continue
			}
			s.consume(ctx, stream, conn)
		}
	}
}

func (s *Slave) consume(ctx context.Context, stream pb.TrendService_StreamTopClient, conn *grpc.ClientConn) {
	defer func() {
		if err := conn.Close(); err != nil {
			s.logger.Debug("grpc conn close error", "err", err)
		}
	}()
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
