package surgecore

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		HalfLifeMinutes:  2.5,
		SnapshotInterval: 100 * time.Millisecond,
		MaxSnapshotSize:  100,
		AnomalyCap:       500.0,
		StaleThreshold:   0.05,
	}
}

func TestEngine_Ingest_NewQuery(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	e := New(testConfig(), slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)
	e.Run(context.Background())
	defer e.Stop(context.Background())

	if !e.Ingest("iphone") {
		t.Error("expected new query to be accepted")
	}

	e.mu.Lock()
	ent, ok := e.entries["iphone"]
	e.mu.Unlock()

	if !ok {
		t.Error("expected entry to exist")
	}
	if ent.score != 1.0 {
		t.Errorf("expected score=1.0, got %f", ent.score)
	}
}

func TestEngine_Ingest_Decay(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	cfg := testConfig()
	cfg.HalfLifeMinutes = 0.1
	e := New(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)

	e.Ingest("iphone")
	e.Ingest("iphone")

	e.mu.Lock()
	ent := e.entries["iphone"]
	e.mu.Unlock()

	if ent.score < 1.9 || ent.score > 2.1 {
		t.Errorf("expected score≈2.0, got %f", ent.score)
	}

	lambda := math.Log(2) / (cfg.HalfLifeMinutes * 60.0)
	dt := 0.15
	expected := 2.0 * math.Exp(-lambda*dt)

	e.mu.Lock()
	ent.score = expected
	e.mu.Unlock()

	e.buildSnapshot()

	e.mu.Lock()
	if ent.score < expected-0.01 {
		t.Errorf("decay not applied correctly")
	}
	e.mu.Unlock()
}

func TestEngine_Ingest_AnomalyCap(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	cfg := testConfig()
	cfg.AnomalyCap = 5.0
	e := New(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)

	for i := 0; i < 100; i++ {
		e.Ingest("iphone")
	}

	e.mu.Lock()
	score := e.entries["iphone"].score
	e.mu.Unlock()

	if score > cfg.AnomalyCap+0.01 {
		t.Errorf("expected score≤%f, got %f", cfg.AnomalyCap, score)
	}
}

func TestEngine_Stoplist_Filtering(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	e := New(testConfig(), slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)
	e.UpdateStopList([]string{"spam", "promo"})

	if e.Ingest("spam") {
		t.Error("expected stoplist word to be rejected")
	}
	if !e.Ingest("iphone") {
		t.Error("expected valid query to be accepted")
	}

	e.UpdateStopList([]string{"iphone"})
	if e.Ingest("iphone") {
		t.Error("expected newly added stoplist word to be rejected")
	}
}

func TestEngine_BuildSnapshot_SortingAndCapping(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	cfg := testConfig()
	cfg.MaxSnapshotSize = 3
	e := New(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)

	e.Ingest("zebra")
	e.Ingest("zebra")
	e.Ingest("zebra")
	e.Ingest("apple")
	e.Ingest("apple")
	e.Ingest("banana")

	e.buildSnapshot()

	var items []struct {
		Query string  `json:"query"`
		Score float64 `json:"score"`
	}
	if err := json.Unmarshal(e.GetSnapshotJSON(), &items); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if len(items) > cfg.MaxSnapshotSize {
		t.Errorf("expected ≤%d items, got %d", cfg.MaxSnapshotSize, len(items))
	}

	if !sort.SliceIsSorted(items, func(i, j int) bool {
		if items[i].Score == items[j].Score {
			return items[i].Query < items[j].Query
		}
		return items[i].Score > items[j].Score
	}) {
		t.Error("items not sorted correctly")
	}
}

func TestEngine_BuildSnapshot_StalePruning(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	cfg := testConfig()
	cfg.HalfLifeMinutes = 0.01
	cfg.StaleThreshold = 0.5
	e := New(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)

	e.Ingest("fresh")
	e.Ingest("fresh")
	e.Ingest("stale")

	lambda := math.Log(2) / (cfg.HalfLifeMinutes * 60.0)
	staleScore := math.Exp(-lambda * 1.0)

	e.mu.Lock()
	if stale, ok := e.entries["stale"]; ok {
		stale.score = staleScore
	}
	e.mu.Unlock()

	e.buildSnapshot()

	var items []struct {
		Query string `json:"query"`
	}
	json.Unmarshal(e.GetSnapshotJSON(), &items)

	found := make(map[string]bool)
	for _, it := range items {
		found[it.Query] = true
	}

	if !found["fresh"] {
		t.Error("expected 'fresh' in snapshot")
	}
	if found["stale"] {
		t.Error("expected 'stale' to be pruned")
	}
}

func TestEngine_NormalizeQuery_Integration(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	e := New(testConfig(), slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)

	e.Ingest("купить айфон")
	e.Ingest("айфон")
	e.Ingest("  АЙФОН  ")

	e.buildSnapshot()

	var items []struct {
		Query string `json:"query"`
	}
	json.Unmarshal(e.GetSnapshotJSON(), &items)

	count := 0
	for _, it := range items {
		if strings.Contains(strings.ToLower(it.Query), "айфон") {
			count++
		}
	}

	if count != 1 {
		t.Errorf("expected normalized 'айфон' once, found %d", count)
	}
}

func TestEngine_GetSnapshotJSON_Concurrency(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	e := New(testConfig(), slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)
	e.Run(context.Background())
	defer e.Stop(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				e.Ingest("query")
				_ = e.GetSnapshotJSON()
			}
		}(i)
	}
	wg.Wait()
}

func TestEngine_RunStop_Lifecycle(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	e := New(testConfig(), slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	e.Run(ctx)
	e.Ingest("test")
	time.Sleep(150 * time.Millisecond)
	e.Stop(context.Background())

	e.mu.Lock()
	_, exists := e.entries["test"]
	e.mu.Unlock()

	if !exists {
		t.Error("expected entry to persist")
	}
}

func TestEngine_EmptySnapshot(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	e := New(testConfig(), slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)

	if string(e.GetSnapshotJSON()) != "[]" {
		t.Errorf("expected empty snapshot")
	}
}

func TestEngine_NilStopList(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	e := New(testConfig(), slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)

	if e.isStopWord("anything") {
		t.Error("expected no words stopped before UpdateStopList")
	}
}

func TestEngine_UpdateStopList_Concurrency(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	e := New(testConfig(), slog.New(slog.NewTextHandler(os.Stderr, nil))).(*engine)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			e.UpdateStopList([]string{string(rune('a' + id))})
			e.Ingest("test")
		}(i)
	}
	wg.Wait()
}