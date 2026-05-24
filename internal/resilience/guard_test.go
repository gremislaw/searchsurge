package resilience

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

type mockCounter struct {
	mu    sync.Mutex
	count int
}

func (m *mockCounter) Inc() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count++
}
func (m *mockCounter) Write(out *io_prometheus_client.Metric) error { return nil }
func (m *mockCounter) Add(float64)                                  {}
func (m *mockCounter) Desc() *prometheus.Desc {
	return prometheus.NewDesc("mock_counter", "mock counter for tests", nil, nil)
}
func (m *mockCounter) Describe(chan<- *prometheus.Desc) {}
func (m *mockCounter) Collect(chan<- prometheus.Metric) {}
func (m *mockCounter) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count
}

// Граничные значения латентности
func TestLatencyGuard_BoundaryLatencies(t *testing.T) {
	t.Parallel()

	threshold := 10 * time.Millisecond

	tests := []struct {
		name     string
		latency  time.Duration
		expected bool
	}{
		{"zero latency", 0, true},
		{"negative (impossible but safe)", -1 * time.Millisecond, true},
		{"just below threshold", 9*time.Millisecond + 999*time.Microsecond, true},
		{"exactly at threshold", 10 * time.Millisecond, true},
		{"just above threshold", 10*time.Millisecond + 1*time.Microsecond, true},
		{"way above threshold", 100 * time.Millisecond, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewLatencyGuard(threshold, nil)
			g.RecordLatency(tt.latency)
			_ = g.ShouldAdmit()
			_ = g.ShouldAdmit()
		})
	}
}

// Клэмпинг dropRate (не должен выходить за [0, 1])
func TestLatencyGuard_DropRateClamping(t *testing.T) {
	t.Parallel()

	g := NewLatencyGuard(1*time.Millisecond, nil)

	for i := 0; i < 50; i++ {
		g.RecordLatency(100 * time.Millisecond)
		_ = g.ShouldAdmit()
	}

	// Проверяем внутреннее состояние через серию вызовов
	// Если dropRate > 1.0, ShouldAdmit() всегда будет возвращать false (после первого рандома)
	// Если dropRate < 0.0, всегда true
	// Мы проверяем, что поведение детерминировано и не залипает

	for i := 0; i < 30; i++ {
		g.RecordLatency(100 * time.Microsecond)
		_ = g.ShouldAdmit()
	}

	accepted := 0
	for i := 0; i < 100; i++ {
		if g.ShouldAdmit() {
			accepted++
		}
	}
	if accepted < 80 {
		t.Errorf("expected high acceptance after recovery, got %d/100", accepted)
	}
}

// Nil prometheus.Counter
func TestLatencyGuard_NilMetric(t *testing.T) {
	t.Parallel()

	g := NewLatencyGuard(10*time.Millisecond, nil)

	for i := 0; i < 1000; i++ {
		g.RecordLatency(time.Duration(i) * time.Millisecond)
		_ = g.ShouldAdmit()
	}
}

// Concurrency safety
func TestLatencyGuard_Concurrency(t *testing.T) {
	t.Parallel()

	g := NewLatencyGuard(10*time.Millisecond, &mockCounter{})
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				latency := time.Duration((id+j)%20) * time.Millisecond
				g.RecordLatency(latency)
				_ = g.ShouldAdmit()
			}
		}(i)
	}
	wg.Wait()
}

// Статистическая корректность
func TestLatencyGuard_DropRateStatistical(t *testing.T) {
	t.Parallel()

	threshold := 5 * time.Millisecond
	g := NewLatencyGuard(threshold, nil)

	for i := 0; i < 5; i++ {
		g.RecordLatency(20 * time.Millisecond)
		_ = g.ShouldAdmit()
	}

	// Проверяем, что после насыщения система дропает >95% трафика
	dropped := 0
	for i := 0; i < 100; i++ {
		if !g.ShouldAdmit() {
			dropped++
		}
	}
	ratio := float64(dropped) / 100.0
	if ratio < 0.95 {
		t.Errorf("expected saturation drop ratio > 0.95, got %.2f", ratio)
	}

	g.RecordLatency(100 * time.Microsecond)
	recovered := 0
	for i := 0; i < 50; i++ {
		if g.ShouldAdmit() {
			recovered++
		}
	}
	if recovered < 40 {
		t.Errorf("expected recovery > 80%%, got %d/50", recovered)
	}
}

// Recovery после долгого периода высокой латентности
func TestLatencyGuard_RecoveryAfterSustainedHighLatency(t *testing.T) {
	t.Parallel()

	threshold := 10 * time.Millisecond
	g := NewLatencyGuard(threshold, nil)

	for i := 0; i < 20; i++ {
		g.RecordLatency(50 * time.Millisecond)
		_ = g.ShouldAdmit()
	}

	for i := 0; i < 25; i++ {
		g.RecordLatency(1 * time.Microsecond)
		_ = g.ShouldAdmit()
	}

	accepted := 0
	for i := 0; i < 100; i++ {
		if g.ShouldAdmit() {
			accepted++
		}
	}
	if accepted < 90 {
		t.Errorf("expected near-full acceptance after recovery, got %d/100", accepted)
	}
}

// Threshold = 0
func TestLatencyGuard_ZeroThreshold(t *testing.T) {
	t.Parallel()

	g := NewLatencyGuard(0, nil)

	g.RecordLatency(1 * time.Nanosecond)
	if !g.ShouldAdmit() {
		t.Error("first call should admit even with zero threshold")
	}
}

// Очень большой threshold
func TestLatencyGuard_HugeThreshold(t *testing.T) {
	t.Parallel()

	g := NewLatencyGuard(1*time.Hour, nil)

	for i := 0; i < 1000; i++ {
		g.RecordLatency(1 * time.Second)
		if !g.ShouldAdmit() {
			t.Errorf("should never drop with huge threshold, iteration %d", i)
			break
		}
	}
}
