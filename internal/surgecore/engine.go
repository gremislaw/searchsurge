package surgecore

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "searchsurge/internal/pb/proto"
	"searchsurge/internal/shared"
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

type engine struct {
	cfg       Config
	logger    shared.Logger
	mu        sync.RWMutex
	entries   map[string]*entry
	metrics   shared.MetricsObserver
	stopList  atomic.Pointer[map[string]struct{}]
	snapJSON  atomic.Pointer[[]byte]
	snapProto atomic.Pointer[pb.GetTopResponse]

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type Engine interface {
	Ingest(query string) bool
	GetSnapshotJSON() []byte
	GetSnapshotProto() *pb.GetTopResponse
	UpdateStopList(words []string)
	Run(ctx context.Context)
	Stop(ctx context.Context)
}

func New(cfg Config, logger shared.Logger, metrics shared.MetricsObserver) Engine {
	return &engine{
		cfg:     cfg,
		logger:  logger,
		entries: make(map[string]*entry, 4096),
		metrics: metrics,
	}
}

func (e *engine) Run(ctx context.Context) {
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.runAggregator(e.ctx)
	}()
}

func (e *engine) Stop(ctx context.Context) {
	e.cancel()
	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (e *engine) Ingest(query string) bool {
	q := NormalizeQuery(query)
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

func (e *engine) UpdateStopList(words []string) {
	m := make(map[string]struct{}, len(words))
	for _, w := range words {
		m[strings.TrimSpace(strings.ToLower(w))] = struct{}{}
	}
	e.stopList.Store(&m)
}

func (e *engine) isStopWord(q string) bool {
	m := e.stopList.Load()
	if m == nil {
		return false
	}
	if _, ok := (*m)[q]; ok {
		return true
	}
	words := strings.Fields(q)
	for _, w := range words {
		if _, ok := (*m)[w]; ok {
			return true
		}
	}
	return false
}

func (e *engine) GetSnapshotJSON() []byte {
	if b := e.snapJSON.Load(); b != nil {
		return *b
	}
	return []byte("[]")
}

func (e *engine) GetSnapshotProto() *pb.GetTopResponse {
	if p := e.snapProto.Load(); p != nil {
		return p
	}
	return &pb.GetTopResponse{}
}

func (e *engine) runAggregator(ctx context.Context) {
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

func (e *engine) buildSnapshot() {
	lambda := math.Log(2) / (e.cfg.HalfLifeMinutes * 60.0)
	now := time.Now()

	// Быстрое копирование состояния под RLock
	e.mu.RLock()
	queries := make([]string, 0, len(e.entries))
	scores := make([]float64, 0, len(e.entries))
	lastUpds := make([]time.Time, 0, len(e.entries))
	for q, ent := range e.entries {
		queries = append(queries, q)
		scores = append(scores, ent.score)
		lastUpds = append(lastUpds, ent.lastUpd)
	}
	e.mu.RUnlock()

	// Тяжёлые вычисления БЕЗ ЛОКА
	type processed struct {
		q       string
		score   float64
		lastUpd time.Time
		isStale bool
	}

	type entrySnap struct {
		q     string
		score float64
	}

	processedEntries := make([]processed, 0, len(queries))
	var snap []entrySnap

	for i := range queries {
		dt := now.Sub(lastUpds[i]).Seconds()
		if dt < 0 {
			dt = 0
		}
		newScore := scores[i] * math.Exp(-lambda*dt)
		isStale := newScore < e.cfg.StaleThreshold

		processedEntries = append(processedEntries, processed{
			q: queries[i], score: newScore, lastUpd: now, isStale: isStale,
		})

		if !isStale && newScore > 0.1 {
			snap = append(snap, entrySnap{q: queries[i], score: newScore})
		}
	}

	sort.Slice(snap, func(i, j int) bool {
		if snap[i].score == snap[j].score {
			return snap[i].q < snap[j].q
		}
		return snap[i].score > snap[j].score
	})
	if len(snap) > e.cfg.MaxSnapshotSize {
		snap = snap[:e.cfg.MaxSnapshotSize]
	}

	type item struct {
		Query string  `json:"query"`
		Score float64 `json:"score"`
	}
	items := make([]item, len(snap))
	for i, v := range snap {
		items[i] = item{Query: v.q, Score: math.Round(v.score*100) / 100}
	}
	rawJSON, _ := json.Marshal(items)

	protoItems := make([]*pb.TrendItem, len(snap))
	for i, v := range snap {
		protoItems[i] = &pb.TrendItem{Query: v.q, Score: v.score}
	}

	// Кратковременный Lock для применения изменений
	e.mu.Lock()
	for _, pe := range processedEntries {
		if pe.isStale {
			delete(e.entries, pe.q)
		} else if old, ok := e.entries[pe.q]; ok {
			if !old.lastUpd.After(pe.lastUpd) {
				old.score = pe.score
				old.lastUpd = pe.lastUpd
			}
		}
	}
	e.snapJSON.Store(&rawJSON)
	e.snapProto.Store(&pb.GetTopResponse{Items: protoItems})
	e.mu.Unlock()

	if e.metrics != nil {
		e.metrics.SetActiveEntries(len(e.entries))
		e.metrics.SetTopSnapshotSize(len(snap))
	}
}
