//go:build loadtest
// +build loadtest

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBaseURL     = "http://127.0.0.1:8081"
	defaultDurationSec = 90
	defaultConcurrency = 500
	defaultTickRateMs  = 50
	defaultTimeoutSec  = 5
	warmupTimeoutSec   = 30
	maxRetries         = 3
)

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultValue
}

func waitForServer(ctx context.Context, client *http.Client, baseURL string) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeoutCtx, cancel := context.WithTimeout(ctx, warmupTimeoutSec*time.Second)
	defer cancel()

	log.Printf("Waiting for server at %s/health (timeout: %ds)", baseURL, warmupTimeoutSec)

	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("server not available after %ds", warmupTimeoutSec)
		case <-ticker.C:
			resp, err := client.Get(baseURL + "/health")
			if err != nil {
				log.Printf("Health check failed (retrying): %v", err)
				continue
			}
			if resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				log.Println("Server is ready")
				return nil
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("Health check returned %d: %s (retrying)", resp.StatusCode, string(body))
		}
	}
}

func main() {
	baseURL := getEnv("BASE_URL", defaultBaseURL)
	durationSec := getEnvInt("DURATION_SEC", defaultDurationSec)
	concurrency := getEnvInt("CONCURRENCY", defaultConcurrency)
	tickRateMs := getEnvInt("TICK_RATE_MS", defaultTickRateMs)

	duration := time.Duration(durationSec) * time.Second
	tickRate := time.Duration(tickRateMs) * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	client := &http.Client{
		Timeout: time.Duration(defaultTimeoutSec) * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
			MaxConnsPerHost:     200,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   false,
			ForceAttemptHTTP2:   true,
		},
	}

	if err := waitForServer(ctx, client, baseURL); err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}

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
					url := baseURL + "/health"
					if id%2 == 0 {
						url = baseURL + "/top?n=10"
					}

					var resp *http.Response
					var err error
					var elapsed float64

					for attempt := 0; attempt < maxRetries; attempt++ {
						t0 := time.Now()
						resp, err = client.Get(url)
						elapsed = time.Since(t0).Seconds() * 1000

						if err == nil && resp.StatusCode == http.StatusOK {
							atomic.AddInt64(&total, 1)
							atomic.AddInt64(&success, 1)
							io.Copy(io.Discard, resp.Body)
							resp.Body.Close()

							mu.Lock()
							latencies = append(latencies, elapsed)
							mu.Unlock()
							break
						}

						if err != nil {
							if strings.Contains(err.Error(), "can't assign requested address") ||
								strings.Contains(err.Error(), "no free ports") {
								if attempt < maxRetries-1 {
									time.Sleep(50 * time.Millisecond)
									continue
								}
							}
							log.Printf("Request error: %v", err)
							atomic.AddInt64(&total, 1)
							atomic.AddInt64(&fail, 1)
						} else if resp != nil {
							log.Printf("HTTP %d for %s", resp.StatusCode, url)
							io.Copy(io.Discard, resp.Body)
							resp.Body.Close()
							atomic.AddInt64(&total, 1)
							atomic.AddInt64(&fail, 1)
						}
						break
					}
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
