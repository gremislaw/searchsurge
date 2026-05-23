package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Role             string
	MasterAddr       string
	HTTPAddr         string
	GRPCAddr         string
	BrokerURL        string
	BrokerSubject    string
	BrokerGroup      string
	HalfLifeMinutes  float64
	SnapshotInterval time.Duration
	MaxSnapshotSize  int
	AnomalyCap       float64
	StaleThreshold   float64
	LogLevel         string
	PrometheusAddr   string
}

func Load() Config {
	return Config{
		Role:             requireEnv("ROLE"),
		HTTPAddr:         requireEnv("HTTP_ADDR"),
		GRPCAddr:         requireEnv("GRPC_ADDR"),
		BrokerURL:        requireEnv("BROKER_URL"),
		BrokerSubject:    requireEnv("BROKER_SUBJECT"),
		BrokerGroup:      requireEnv("BROKER_GROUP"),
		MasterAddr:       os.Getenv("MASTER_ADDR"),
		HalfLifeMinutes:  requireFloat("HALF_LIFE_MIN"),
		SnapshotInterval: requireDurationMs("SNAPSHOT_INTERVAL_MS"),
		MaxSnapshotSize:  requireInt("MAX_SNAPSHOT_SIZE"),
		AnomalyCap:       requireFloat("ANOMALY_CAP"),
		StaleThreshold:   requireFloat("STALE_THRESHOLD"),
		LogLevel:         os.Getenv("LOG_LEVEL"),
		PrometheusAddr:   os.Getenv("PROMETHEUS_ADDR"),
	}
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("missing required env: " + key)
	}
	return v
}

func requireInt(key string) int {
	v := requireEnv(key)
	n, err := strconv.Atoi(v)
	if err != nil {
		panic("invalid int for " + key + ": " + v)
	}
	return n
}

func requireFloat(key string) float64 {
	v := requireEnv(key)
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic("invalid float for " + key + ": " + v)
	}
	return f
}

func requireDurationMs(key string) time.Duration {
	return time.Duration(requireInt(key)) * time.Millisecond
}