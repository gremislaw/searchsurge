package resilience

import (
	"context"
	"searchsurge/internal/shared"
	"sync"
	"testing"
	"time"

	pb "searchsurge/internal/pb/proto"
)

type mockEngine struct {
	mu            sync.Mutex
	ingests       []string
	stoplist      map[string]struct{}
	snapshotJSON  []byte
	snapshotProto *pb.GetTopResponse
}

func (m *mockEngine) Ingest(query string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ingests = append(m.ingests, query)
	_, blocked := m.stoplist[query]
	return !blocked
}

func (m *mockEngine) GetSnapshotJSON() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotJSON
}

func (m *mockEngine) GetSnapshotProto() *pb.GetTopResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotProto
}

func (m *mockEngine) UpdateStopList(words []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stoplist == nil {
		m.stoplist = make(map[string]struct{})
	}
	for _, w := range words {
		m.stoplist[w] = struct{}{}
	}
}

func (m *mockEngine) Run(ctx context.Context)  {}
func (m *mockEngine) Stop(ctx context.Context) {}
func (m *mockEngine) Stats() (int, int)        { return 0, 0 }

func TestProtectedEngine_Ingest_AdmittedAndAccepted(t *testing.T) {
	t.Parallel()

	core := &mockEngine{snapshotJSON: []byte("[]")}
	guardDroppedCounter := &shared.NoopMetricsObserver{}
	pe := NewProtectedEngine(core, 100*time.Millisecond, guardDroppedCounter)

	accepted := pe.Ingest("iphone")
	if !accepted {
		t.Error("expected ingest to be accepted")
	}
	core.mu.Lock()
	if len(core.ingests) != 1 || core.ingests[0] != "iphone" {
		t.Errorf("expected core.Ingest called with 'iphone', got %v", core.ingests)
	}
	core.mu.Unlock()
}

func TestProtectedEngine_Ingest_AdmittedButDroppedByCore(t *testing.T) {
	t.Parallel()

	core := &mockEngine{
		stoplist:     map[string]struct{}{"spam": {}},
		snapshotJSON: []byte("[]"),
	}
	guardDroppedCounter := &shared.NoopMetricsObserver{}
	pe := NewProtectedEngine(core, 100*time.Millisecond, guardDroppedCounter)

	accepted := pe.Ingest("spam")
	if accepted {
		t.Error("expected spam to be dropped by stoplist")
	}
	core.mu.Lock()
	if len(core.ingests) != 1 {
		t.Error("expected core.Ingest to be called even for stoplist word")
	}
	core.mu.Unlock()
}

func TestProtectedEngine_Ingest_DroppedByGuard(t *testing.T) {
	t.Parallel()

	core := &mockEngine{snapshotJSON: []byte("[]")}
	guardDroppedCounter := &shared.NoopMetricsObserver{}
	pe := NewProtectedEngine(core, 1*time.Nanosecond, guardDroppedCounter)
	pe.guard.RecordLatency(1 * time.Second)

	for i := 0; i < 15; i++ {
		_ = pe.guard.ShouldAdmit()
	}

	accepted := pe.Ingest("iphone")
	if accepted {
		t.Error("expected ingest to be dropped by guard")
	}
	core.mu.Lock()
	if len(core.ingests) != 0 {
		t.Errorf("expected core.Ingest NOT called when guard drops, got %v", core.ingests)
	}
	core.mu.Unlock()
}

func TestProtectedEngine_GetSnapshotJSON_RecordsLatency(t *testing.T) {
	t.Parallel()

	core := &mockEngine{snapshotJSON: []byte(`[{"query":"test","score":1.0}]`)}
	guardDroppedCounter := &shared.NoopMetricsObserver{}
	pe := NewProtectedEngine(core, 10*time.Millisecond, guardDroppedCounter)

	snap := pe.GetSnapshotJSON()
	if string(snap) != `[{"query":"test","score":1.0}]` {
		t.Errorf("unexpected snapshot: %s", snap)
	}
	pe.guard.mu.Lock()
	if pe.guard.lastLatency == 0 {
		t.Error("expected lastLatency to be recorded")
	}
	pe.guard.mu.Unlock()
}

func TestProtectedEngine_UpdateStopList_Delegates(t *testing.T) {
	t.Parallel()

	core := &mockEngine{snapshotJSON: []byte("[]")}
	pe := NewProtectedEngine(core, time.Second, nil)

	pe.UpdateStopList([]string{"promo", "sale"})

	core.mu.Lock()
	if _, ok := core.stoplist["promo"]; !ok {
		t.Error("expected 'promo' in stoplist")
	}
	if _, ok := core.stoplist["sale"]; !ok {
		t.Error("expected 'sale' in stoplist")
	}
	core.mu.Unlock()
}

func TestProtectedEngine_RunStop_Delegates(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	core := &mockEngine{snapshotJSON: []byte("[]")}
	pe := NewProtectedEngine(core, time.Second, nil)

	pe.Run(ctx)
	pe.Stop(ctx)
}

func TestProtectedEngine_NilMetric_DoesNotPanic(t *testing.T) {
	t.Parallel()

	core := &mockEngine{snapshotJSON: []byte("[]")}
	pe := NewProtectedEngine(core, time.Second, nil)

	for i := 0; i < 100; i++ {
		_ = pe.Ingest("query")
		_ = pe.GetSnapshotJSON()
	}
}

func TestProtectedEngine_Concurrency(t *testing.T) {
	t.Parallel()

	core := &mockEngine{snapshotJSON: []byte("[]")}
	guardDroppedCounter := &shared.NoopMetricsObserver{}
	pe := NewProtectedEngine(core, 10*time.Millisecond, guardDroppedCounter)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = pe.Ingest("query")
				_ = pe.GetSnapshotJSON()
				pe.UpdateStopList([]string{"word"})
			}
		}(i)
	}
	wg.Wait()
}

func TestProtectedEngine_GetSnapshotJSON_Empty(t *testing.T) {
	t.Parallel()

	core := &mockEngine{snapshotJSON: []byte("[]")}
	pe := NewProtectedEngine(core, time.Second, nil)

	snap := pe.GetSnapshotJSON()
	if string(snap) != "[]" {
		t.Errorf("expected empty snapshot, got %s", snap)
	}
}
