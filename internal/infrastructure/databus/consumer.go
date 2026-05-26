package databus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"searchsurge/internal/shared"
	"searchsurge/internal/surgecore"
)

type Config struct {
	URL            string
	Subject        string
	StreamName     string
	ConsumerName   string
	IdempotencyTTL time.Duration
	AckWait        time.Duration
}

type Event struct {
	Query          string `json:"query"`
	IdempotencyKey string `json:"idempotency_key"`
	Ts             int64  `json:"ts"`
}

type DataBus struct {
	cfg      Config
	engine   surgecore.Engine
	logger   shared.Logger
	metrics  shared.MetricsObserver
	mu       sync.Mutex
	seenKeys map[string]time.Time
}

func New(cfg Config, engine surgecore.Engine, logger shared.Logger, metrics shared.MetricsObserver) *DataBus {
	return &DataBus{
		cfg: cfg, engine: engine, logger: logger, metrics: metrics,
		seenKeys: make(map[string]time.Time, 4096),
	}
}

func (b *DataBus) Run(ctx context.Context) error {
	nc, err := nats.Connect(b.cfg.URL)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream init: %w", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      b.cfg.StreamName,
		Subjects:  []string{b.cfg.Subject},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    shared.EventMaxAge,
		Storage:   jetstream.MemoryStorage,
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	consumer, err := js.CreateOrUpdateConsumer(ctx, b.cfg.StreamName, jetstream.ConsumerConfig{
		Name:      b.cfg.ConsumerName,
		Durable:   b.cfg.ConsumerName,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   b.cfg.AckWait,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	b.startCleaner(ctx)

	_, err = consumer.Consume(func(msg jetstream.Msg) {
		select {
		case <-ctx.Done():
			return
		default:
			b.handleMessage(msg)
		}
	})
	if err != nil {
		return fmt.Errorf("consume start: %w", err)
	}

	<-ctx.Done()
	return nil
}

func (b *DataBus) handleMessage(msg jetstream.Msg) {
	defer func() {
		if r := recover(); r != nil {
			b.logger.Error("handleMessage panic", "err", r)
			if b.metrics != nil {
				b.metrics.EventProcessed("dropped")
			}
			if err := msg.Nak(); err != nil {
				b.logger.Debug("nak failed", "err", err)
			}
		}
	}()

	var evt Event
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		b.logger.Debug("parse error", "err", err)
		if b.metrics != nil {
			b.metrics.IngestDropped("parse_error")
		}
		if err := msg.Ack(); err != nil {
			b.logger.Debug("ack failed", "err", err)
		}
		return
	}

	if evt.Query == "" || evt.IdempotencyKey == "" {
		b.logger.Debug("empty field", "query", evt.Query, "key", evt.IdempotencyKey)
		if b.metrics != nil {
			b.metrics.IngestDropped("empty")
		}
		if err := msg.Ack(); err != nil {
			b.logger.Debug("ack failed", "err", err)
		}
		return
	}

	if !b.checkAndMark(evt.IdempotencyKey) {
		if b.metrics != nil {
			b.metrics.IngestDropped("idempotency")
		}
		if err := msg.Ack(); err != nil {
			b.logger.Debug("ack failed", "err", err)
		}
		return
	}

	accepted := b.engine.Ingest(evt.Query)
	if b.metrics != nil {
		if accepted {
			b.metrics.EventProcessed("accepted")
		} else {
			b.metrics.EventProcessed("dropped")
		}
	}
	if err := msg.Ack(); err != nil {
		b.logger.Debug("ack failed", "err", err)
	}
}

func (b *DataBus) checkAndMark(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.seenKeys[key]; exists {
		return false
	}
	b.seenKeys[key] = time.Now()
	return true
}

func (b *DataBus) startCleaner(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(b.cfg.IdempotencyTTL / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.sweep()
			}
		}
	}()
}

func (b *DataBus) sweep() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	for k, t := range b.seenKeys {
		if now.Sub(t) > b.cfg.IdempotencyTTL {
			delete(b.seenKeys, k)
		}
	}
}
