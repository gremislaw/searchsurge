//go:build loadtest
// +build loadtest

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	natsURL       = "nats://127.0.0.1:4222"
	subject       = "search.events"
	workers       = 20
	targetRPS     = 5000
	durationSecs  = 120
)

type Event struct {
	Query          string `json:"query"`
	IdempotencyKey string `json:"idempotency_key"`
	Ts             int64  `json:"ts"`
}

var queries = []string{
	"купить iphone", "samsung galaxy", "airpods pro", "macbook air",
	"чехол айфон", "xiaomi redmi", "ноутбук gaming", "презики", "самса",
	"ipad mini", "apple watch", "google pixel", "huawei p60", "oneplus 12",
	"найти телефон", "дешевый планшет", "обзор ноутбука", "подобрать наушники",
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durationSecs)*time.Second)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived interrupt, stopping...")
		cancel()
	}()

	nc, err := nats.Connect(natsURL, nats.MaxReconnects(5))
	if err != nil {
		log.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	var pub, fail int64
	start := time.Now()

	tickInterval := time.Duration(int64(time.Second) * int64(workers) / int64(targetRPS))

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			ticker := time.NewTicker(tickInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					q := queries[rng.Intn(len(queries))]
					key := fmt.Sprintf("load-%d-%d", time.Now().UnixNano(), id)
					data, err := json.Marshal(Event{Query: q, IdempotencyKey: key, Ts: time.Now().UnixMilli()})
					if err != nil {
						atomic.AddInt64(&fail, 1)
						continue
					}
					if err := nc.Publish(subject, data); err != nil {
						atomic.AddInt64(&fail, 1)
					} else {
						atomic.AddInt64(&pub, 1)
					}
				}
			}
		}(w)
	}

	wg.Wait()

	elapsed := time.Since(start)
	p := atomic.LoadInt64(&pub)
	f := atomic.LoadInt64(&fail)

	fmt.Printf("\nIngest load finished\n")
	fmt.Printf("Duration:  %s\n", elapsed.Truncate(time.Second))
	fmt.Printf("Published: %d | Failed: %d\n", p, f)
	fmt.Printf("Throughput: %.1f msg/s\n", float64(p)/elapsed.Seconds())
}