package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"searchsurge/internal/api"
	grpcapi "searchsurge/internal/api/grpc"
	httpapi "searchsurge/internal/api/http"
	"searchsurge/internal/config"
	"searchsurge/internal/databus"
	"searchsurge/internal/metrics"
	"searchsurge/internal/replicator"
	"searchsurge/internal/resilience"
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

	// latency guard + circuit breaker обертка
	droppedMetric := metrics.IngestDropped.WithLabelValues("latency_guard")
	engine := resilience.NewProtectedEngine(core, 15*time.Millisecond, droppedMetric)

	var provider api.TrendProvider

	if cfg.Role == "master" {
		busCfg := databus.Config{
			URL:            cfg.BrokerURL,
			Subject:        cfg.BrokerSubject,
			StreamName:     "search_trends",
			ConsumerName:   "surge_master",
			IdempotencyTTL: 5 * time.Minute,
			AckWait:        30 * time.Second,
		}
		master := replicator.NewMaster(engine, busCfg, logger)
		master.Run(ctx)
		provider = master
		logger.Info("started as master")
	} else {
		slave := replicator.NewSlave(cfg.MasterAddr, logger)
		slave.Run(ctx)
		provider = slave
		logger.Info("started as slave", "master_addr", cfg.MasterAddr)
	}

	grpcSrv := grpc.NewServer()
	grpcServer := grpcapi.NewServer(provider)
	grpcServer.Register(grpcSrv)

	httpHandler := httpapi.NewRouter(provider)

	httpSrv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      httpHandler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	promSrv := &http.Server{
		Addr:         cfg.PrometheusAddr,
		Handler:      promhttp.Handler(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			logger.Error("gRPC listen failed", "err", err)
			os.Exit(1)
		}
		logger.Info("gRPC server started", "addr", cfg.GRPCAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			logger.Error("gRPC server error", "err", err)
		}
	}()

	go func() {
		logger.Info("HTTP server started", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "err", err)
		}
	}()

	go func() {
		logger.Info("Prometheus metrics started", "addr", cfg.PrometheusAddr)
		if err := promSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Prometheus server error", "err", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown initiated")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Graceful shutdown
	core.Stop(shutdownCtx)
	grpcSrv.GracefulStop()
	httpSrv.Shutdown(shutdownCtx)
	promSrv.Shutdown(shutdownCtx)

	logger.Info("shutdown complete")
}

func initLogger(level string) *slog.Logger {
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