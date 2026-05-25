//go:build integration
// +build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	httpBase      = "http://localhost:8081"
	natsURL       = "nats://localhost:4222"
	subject       = "search.events"
)

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION") == "" {
		fmt.Println("Skipping integration tests. Set INTEGRATION=1 to run.")
		os.Exit(0)
	}
	code := m.Run()
	os.Exit(code)
}

type responseTop struct {
	Items []struct {
		Query string  `json:"query"`
		Score float64 `json:"score"`
	} `json:"items"`
}

func fetchTop(t *testing.T, url string) responseTop {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("http get %s: %v", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}

	var respTop responseTop
	if err := json.Unmarshal(body, &respTop); err != nil {
		t.Fatalf("unmarshal: %v (body: %s)", err, body)
	}
	return respTop
}

func TestE2E_IngestAndTop(t *testing.T) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	events := []struct {
		Query          string `json:"query"`
		IdempotencyKey string `json:"idempotency_key"`
		Ts             int64  `json:"ts"`
	}{
		{"iphone 15", "k1", time.Now().UnixMilli()},
		{"iphone 15", "k2", time.Now().UnixMilli()},
		{"samsung galaxy", "k3", time.Now().UnixMilli()},
		{"купить айфон", "k4", time.Now().UnixMilli()},
	}

	for _, e := range events {
		data, _ := json.Marshal(e)
		if err := nc.Publish(subject, data); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	nc.Flush()

	time.Sleep(3 * time.Second)

	top := fetchTop(t, httpBase+"/top?n=5")

	if len(top.Items) == 0 {
		t.Fatal("expected non-empty top")
	}

	found := make(map[string]bool)
	for _, it := range top.Items {
		found[it.Query] = true
	}

	if !found["iphone 15"] {
		t.Error("expected 'iphone 15' in top")
	}

	for _, it := range top.Items {
		if strings.HasPrefix(it.Query, "купить ") || strings.HasPrefix(it.Query, "заказать ") {
			t.Errorf("intent word not filtered: %q", it.Query)
		}
	}
}

func TestE2E_Stoplist(t *testing.T) {
	payload := `{"words":["promo","sale"]}`
	resp, err := http.Post(
		httpBase+"/stoplist",
		"application/json",
		bytes.NewBufferString(payload),
	)
	if err != nil {
		t.Fatalf("post stoplist: %v", err)
	}
	resp.Body.Close()

	time.Sleep(500 * time.Millisecond)

	nc, _ := nats.Connect(natsURL)
	defer nc.Close()

	data, _ := json.Marshal(map[string]any{
		"query": "promo iphone", "idempotency_key": "k99", "ts": time.Now().UnixMilli(),
	})
	nc.Publish(subject, data)
	nc.Flush()

	time.Sleep(2 * time.Second)

	top := fetchTop(t, httpBase+"/top?n=5")

	for _, it := range top.Items {
		if strings.Contains(strings.ToLower(it.Query), "promo") {
			t.Errorf("stoplist failed: %q found in response", it.Query)
		}
	}
}

func TestE2E_PrometheusScraping(t *testing.T) {
	url := "http://localhost:9095/api/v1/query?query=searchsurge_active_entries"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("prometheus api: %v", err)
	}
	defer resp.Body.Close()

	var promResp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	json.NewDecoder(resp.Body).Decode(&promResp)

	if promResp.Status != "success" {
		t.Errorf("prometheus query failed: %+v", promResp)
	}
}

func TestE2E_MasterSlaveSync(t *testing.T) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	uniqueQuery := fmt.Sprintf("sync-test-%d", time.Now().UnixNano())

	// Burst-инжест
	for i := 0; i < 20; i++ {
		data, _ := json.Marshal(map[string]any{
			"query":           uniqueQuery,
			"idempotency_key": fmt.Sprintf("sync-key-%d-%d", time.Now().UnixNano(), i),
			"ts":              time.Now().UnixMilli(),
		})
		if err := nc.Publish(subject, data); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	nc.Flush()

	waitForQueryInTop := func(t *testing.T, url string) responseTop {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			resp, err := http.Get(url)
			if err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					var top responseTop
					if err := json.Unmarshal(body, &top); err == nil {
						for _, item := range top.Items {
							if item.Query == uniqueQuery {
								return top // Нашли!
							}
						}
					}
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
		t.Fatalf("timeout: %q not found in %s within 15s", uniqueQuery, url)
		return responseTop{}
	}

	masterTop := waitForQueryInTop(t, httpBase+"/top?n=50")

	slaveURL := "http://127.0.0.1:8082/top?n=50"
	slaveTop := waitForQueryInTop(t, slaveURL)

	foundMaster := false
	for _, it := range masterTop.Items {
		if it.Query == uniqueQuery {
			foundMaster = true
			break
		}
	}
	if !foundMaster {
		t.Error("query not found on master")
	}

	foundSlave := false
	for _, it := range slaveTop.Items {
		if it.Query == uniqueQuery {
			foundSlave = true
			break
		}
	}
	if !foundSlave {
		t.Error("query not found on slave")
	}
}