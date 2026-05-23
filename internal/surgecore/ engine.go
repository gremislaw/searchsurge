package surgecore

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	HalfLifeMinutes  float64
	SnapshotInterval time.Duration
	MaxSnapshotSize  int
	AnomalyCap       float64
	StaleThreshold   float64
}

type entry struct {
	score   float64
	lastUpd time.Time
}

type Engine struct {
	cfg      Config
	logger   *slog.Logger
	mu       sync.Mutex
	entries  map[string]*entry
	stopList atomic.Pointer[map[string]struct{}]
	snapJSON atomic.Pointer[[]byte]

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(cfg Config, logger *slog.Logger) *Engine {
	return &Engine{
		cfg:     cfg,
		logger:  logger,
		entries: make(map[string]*entry, 4096),
	}
}

func (e *Engine) Run(ctx context.Context) {
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.runAggregator(e.ctx)
	}()
}

func (e *Engine) Stop(ctx context.Context) {
	e.cancel()
	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (e *Engine) Ingest(query string) bool {
	q := strings.TrimSpace(strings.ToLower(query))
	if q == "" || e.isStopWord(q) {
		return false
	}

	now := time.Now()
	lambda := math.Log(2) / (e.cfg.HalfLifeMinutes * 60.0)

	e.mu.Lock()
	ent, ok := e.entries[q]
	if !ok {
		e.entries[q] = &entry{score: 1.0, lastUpd: now}
		e.mu.Unlock()
		return true
	}

	dt := now.Sub(ent.lastUpd).Seconds()
	if dt < 0 {
		dt = 0
	}
	ent.score = ent.score*math.Exp(-lambda*dt) + 1.0
	if ent.score > e.cfg.AnomalyCap {
		ent.score = e.cfg.AnomalyCap
	}
	ent.lastUpd = now
	e.mu.Unlock()
	return true
}

func (e *Engine) UpdateStopList(words []string) {
	m := make(map[string]struct{}, len(words))
	for _, w := range words {
		m[strings.TrimSpace(strings.ToLower(w))] = struct{}{}
	}
	e.stopList.Store(&m)
}

func (e *Engine) isStopWord(q string) bool {
	m := e.stopList.Load()
	if m == nil {
		return false
	}
	_, ok := (*m)[q]
	return ok
}

func (e *Engine) GetSnapshotJSON() []byte {
	if b := e.snapJSON.Load(); b != nil {
		return *b
	}
	return []byte("[]")
}

func (e *Engine) runAggregator(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.SnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			e.buildSnapshot()
			return
		case <-ticker.C:
			e.buildSnapshot()
		}
	}
}

func (e *Engine) buildSnapshot() {
	e.mu.Lock()
	defer e.mu.Unlock()

	type item struct {
		Query string  `json:"query"`
		Score float64 `json:"score"`
	}

	items := make([]item, 0, len(e.entries))
	lambda := math.Log(2) / (e.cfg.HalfLifeMinutes * 60.0)
	now := time.Now()

	for q, ent := range e.entries {
		dt := now.Sub(ent.lastUpd).Seconds()
		if dt < 0 {
			dt = 0
		}
		currentScore := ent.score * math.Exp(-lambda*dt)

		if currentScore < e.cfg.StaleThreshold {
			delete(e.entries, q)
			continue
		}

		ent.score = currentScore
		ent.lastUpd = now

		if currentScore > 0.1 {
			items = append(items, item{Query: q, Score: math.Round(currentScore*100) / 100})
		}
	}

	if len(items) == 0 {
		e.snapJSON.Store(&[]byte("[]"))
		return
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Score == items[j].Score {
			return items[i].Query < items[j].Query
		}
		return items[i].Score > items[j].Score
	})

	if len(items) > e.cfg.MaxSnapshotSize {
		items = items[:e.cfg.MaxSnapshotSize]
	}

	raw, err := json.Marshal(items)
	if err != nil {
		return
	}
	e.snapJSON.Store(&raw)
}