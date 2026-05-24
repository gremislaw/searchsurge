package grpc

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func UnaryLoggingInterceptor(logger *slog.Logger, role string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		s, _ := status.FromError(err)
		logger.Info("gRPC unary",
			"role", role,
			"method", info.FullMethod,
			"status", s.Code().String(),
			"latency_ms", time.Since(start).Milliseconds(),
		)
		return resp, err
	}
}

func StreamLoggingInterceptor(logger *slog.Logger, role string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)

		s, _ := status.FromError(err)
		logger.Info("gRPC stream closed",
			"role", role,
			"method", info.FullMethod,
			"status", s.Code().String(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return err
	}
}
