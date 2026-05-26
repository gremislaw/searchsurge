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
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	natsURL              = "nats://127.0.0.1:4222"
	subject              = "search.events"
	defaultWorkers       = 20
	defaultTargetRPS     = 5000
	defaultDurationSecs  = 120
	defaultUniqueQueries = 5000
)

type Event struct {
	Query          string `json:"query"`
	IdempotencyKey string `json:"idempotency_key"`
	Ts             int64  `json:"ts"`
}

func generateQueryPool(count int) []string {
	templates := []string{
		"купить %s", "заказать %s", "цена %s", "обзор %s",
		"%s отзывы", "%s характеристики", "где купить %s", "%s доставка",
	}
	products := []string{
		"iphone", "samsung", "xiaomi", "huawei", "oneplus", "google pixel",
		"macbook", "airpods", "ipad", "apple watch", "redmi", "poco",
		"ноутбук", "планшет", "наушники", "часы", "монитор", "клавиатура",
		"мышь", "вебкамера", "микрофон", "колонки", "роутер", "видеокарта",
	}

	queries := make([]string, 0, count)
	for i := 0; i < count; i++ {
		tmpl := templates[i%len(templates)]
		prod := products[rand.Intn(len(products))]
		suffix := ""
		if i >= len(products)*len(templates) {
			suffix = fmt.Sprintf("-%d", i/100)
		}
		queries = append(queries, fmt.Sprintf(tmpl, prod+suffix))
	}
	return queries
}

func getEnvInt(key string, defaultValue int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultValue
}

func main() {
	workers := getEnvInt("WORKERS", defaultWorkers)
	targetRPS := getEnvInt("TARGET_RPS", defaultTargetRPS)
	durationSecs := getEnvInt("DURATION_SECS", defaultDurationSecs)
	uniqueQueries := getEnvInt("UNIQUE_QUERIES", defaultUniqueQueries)

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

	queries := generateQueryPool(uniqueQueries)
	log.Printf("Generated %d unique queries", len(queries))

	var pub, fail int64
	start := time.Now()

	tickInterval := time.Duration(int64(time.Second) * int64(workers) / int64(targetRPS))
	log.Printf("Workers: %d, Target RPS: %d, Tick interval: %v", workers, targetRPS, tickInterval)

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
					key := fmt.Sprintf("load-%d-%d-%d", time.Now().UnixNano(), id, rng.Intn(10000))
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
	fmt.Printf("Unique queries in pool: %d\n", uniqueQueries)
	fmt.Printf("Published: %d | Failed: %d\n", p, f)
	fmt.Printf("Throughput: %.1f msg/s\n", float64(p)/elapsed.Seconds())
}
