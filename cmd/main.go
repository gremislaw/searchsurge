package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	_ "net/http/pprof"

	"searchsurge/internal/api"
	grpcapi "searchsurge/internal/api/grpc"
	httpapi "searchsurge/internal/api/http"
	"searchsurge/internal/config"
	"searchsurge/internal/infrastructure/databus"
	"searchsurge/internal/infrastructure/replicator"
	"searchsurge/internal/metrics"
	pb "searchsurge/internal/pb/proto"
	"searchsurge/internal/resilience"
	"searchsurge/internal/shared"
	"searchsurge/internal/surgecore"
)

func main() {
	cfg := config.Load()
	logger := initLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics.Register(prometheus.DefaultRegisterer)

	core := surgecore.New(surgecore.Config{
		HalfLifeMinutes:  cfg.HalfLifeMinutes,
		SnapshotInterval: cfg.SnapshotInterval,
		MaxSnapshotSize:  cfg.MaxSnapshotSize,
		AnomalyCap:       cfg.AnomalyCap,
		StaleThreshold:   cfg.StaleThreshold,
	}, logger)

	// обёртка latency guard + circuit breaker
	var metrics shared.MetricsObserver = &shared.NoopMetricsObserver{}
	engine := resilience.NewProtectedEngine(core, 15*time.Millisecond, metrics)

	var provider api.TrendProvider
	var busMetrics databus.MetricsObserver = mainMetrics{}

	if cfg.Role == "master" {
		busCfg := databus.Config{
			URL:            cfg.BrokerURL,
			Subject:        cfg.BrokerSubject,
			StreamName:     shared.StreamName,
			ConsumerName:   shared.ConsumerName,
			IdempotencyTTL: shared.IdempotencyTTL,
			AckWait:        shared.AckWait,
		}
		master := replicator.NewMaster(engine, busCfg, logger, busMetrics)
		master.Run(ctx)
		provider = master
		logger.Info("node started as master")
	} else {
		slave := replicator.NewSlave(cfg.MasterAddr, logger)
		slave.Run(ctx)
		provider = slave
		logger.Info("node started as slave", "master_addr", cfg.MasterAddr)
	}

	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(grpcapi.UnaryLoggingInterceptor(logger, cfg.Role)),
		grpc.StreamInterceptor(grpcapi.StreamLoggingInterceptor(logger, cfg.Role)),
	)

	grpcServer := grpcapi.NewServer(provider)
	grpcServer.Register(grpcSrv)
	reflection.Register(grpcSrv)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("gRPC listen failed", "err", err)
		os.Exit(1)
	}

	go func() {
		logger.Info("gRPC server started", "addr", cfg.GRPCAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			logger.Error("gRPC serve error", "err", err)
		}
	}()

	// gRPC-Gateway
	gwMux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	if err := pb.RegisterTrendServiceHandlerFromEndpoint(ctx, gwMux, cfg.GRPCAddr, opts); err != nil {
		logger.Error("gateway registration failed", "err", err)
		os.Exit(1)
	}

	httpHandler := httpapi.NewRouter(gwMux, cfg.Role)

	httpSrv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      httpHandler,
		ReadTimeout:  shared.ReadTimeout,
		WriteTimeout: shared.WriteTimeout,
		IdleTimeout:  shared.IdleTimeout,
	}

	go func() {
		logger.Info("HTTP server started", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP serve error", "err", err)
		}
	}()

	metricsSrv := &http.Server{
		Addr:         cfg.PrometheusAddr,
		Handler:      promhttp.Handler(),
		ReadTimeout:  shared.ReadTimeout,
		WriteTimeout: shared.WriteTimeout,
	}

	go func() {
		logger.Info("Prometheus metrics started", "addr", cfg.PrometheusAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics serve error", "err", err)
		}
	}()

	debugSrv := &http.Server{
		Addr:         cfg.DebugAddr,
		Handler:      http.DefaultServeMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		logger.Info("debug/pprof started", "addr", cfg.DebugAddr)
		if err := debugSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("debug server failed", "err", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown initiated")

	shCtx, cancel := context.WithTimeout(context.Background(), shared.GracefulShutdownTimeout)
	defer cancel()

	if err := httpSrv.Shutdown(shCtx); err != nil {
		logger.Error("HTTP graceful shutdown failed", "err", err)
	}
	grpcSrv.GracefulStop()
	core.Stop(shCtx)

	if err := debugSrv.Shutdown(shCtx); err != nil {
		logger.Error("pprof graceful shutdown failed", "err", err)
	}

	logger.Info("shutdown complete")
}

func initLogger(level string) shared.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

type mainMetrics struct{}

func (m mainMetrics) EventProcessed(status string) {
	metrics.EventsProcessedTotal.WithLabelValues(status).Inc()
}

func (m mainMetrics) IngestDropped(reason string) {
	metrics.IngestDroppedTotal.WithLabelValues(reason).Inc()
}
