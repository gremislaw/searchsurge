//go:build loadtest
// +build loadtest

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	baseURL     = "http://127.0.0.1:8081"
	duration    = 90 * time.Second
	concurrency = 100
	tickRate    = 50 * time.Millisecond
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	client := &http.Client{Timeout: 5 * time.Second}

	var (
		total, success, fail int64
		mu                   sync.Mutex
		latencies            []float64
	)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ticker := time.NewTicker(tickRate)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					t0 := time.Now()
					url := baseURL + "/health"
					if id%2 == 0 {
						url = baseURL + "/top?n=10"
					}

					resp, err := client.Get(url)
					elapsed := time.Since(t0).Seconds() * 1000

					atomic.AddInt64(&total, 1)
					if err == nil && resp.StatusCode == http.StatusOK {
						atomic.AddInt64(&success, 1)
						resp.Body.Close()
					} else {
						atomic.AddInt64(&fail, 1)
						if err != nil {
							log.Printf("Request error: %v", err)
						} else if resp != nil {
							log.Printf("HTTP %d for %s", resp.StatusCode, url)
							io.Copy(io.Discard, resp.Body)
							resp.Body.Close()
						}
					}

					mu.Lock()
					latencies = append(latencies, elapsed)
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()

	sort.Float64s(latencies)
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	totalR := atomic.LoadInt64(&total)
	okR := atomic.LoadInt64(&success)
	failR := atomic.LoadInt64(&fail)

	fmt.Printf("\nHTTP Load Test Results\n")
	fmt.Printf("Duration:  %s\n", duration.Truncate(time.Second))
	fmt.Printf("RPS:       %.1f\n", float64(totalR)/duration.Seconds())
	fmt.Printf("Success:   %d (%.2f%%)\n", okR, float64(okR)/float64(totalR)*100)
	fmt.Printf("Failed:    %d (%.2f%%)\n", failR, float64(failR)/float64(totalR)*100)
	fmt.Printf("Latency:   p50=%.1fms | p95=%.1fms | p99=%.1fms\n", p50, p95, p99)

	if p95 > 50 || p99 > 100 {
		fmt.Println("WARNING: p95/p99 exceeds SLO (>50ms / >100ms)")
	} else {
		fmt.Println("SLO met: p95 < 50ms, p99 < 100ms")
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
