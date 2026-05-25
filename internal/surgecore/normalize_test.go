package surgecore

import (
	"strings"
	"sync"
	"testing"
)

func resetIntents() {
	m := map[string]struct{}{
		"купить": {}, "заказать": {}, "дешевый": {}, "дешево": {},
		"обзор": {}, "найти": {}, "подобрать": {},
	}
	intentWords.Store(&m)
}

func TestNormalizeQuery(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)

	cases := []struct {
		input    string
		expected string
	}{
		{"купить айфон", "айфон"},
		{"заказать дешевый самсунг", "самсунг"},
		{"  КУПИТЬ Айфон ", "айфон"},
		{"айфон", "айфон"},
		{"купить", "купить"},
		{"", ""},
		{"   ", ""},
		{"купить заказать", "купить заказать"},
		{"iphone 15 pro max", "iphone 15 pro max"},
		{"ноутбук купить", "ноутбук"},
		{"купить   заказать   айфон", "айфон"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			if got := NormalizeQuery(tc.input); got != tc.expected {
				t.Errorf("got %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestUpdateStopIntent(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	UpdateStopIntent([]string{"customintent", "another"})

	if got := NormalizeQuery("customintent phone"); got != "phone" {
		t.Errorf("got %q, want %q", got, "phone")
	}
	if got := NormalizeQuery("another device"); got != "device" {
		t.Errorf("got %q, want %q", got, "device")
	}
}

func TestUpdateStopIntent_Empty(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	UpdateStopIntent([]string{})
	if got := NormalizeQuery("купить айфон"); got != "купить айфон" {
		t.Errorf("got %q, want %q", got, "купить айфон")
	}
}

func TestNormalizeQuery_Concurrency(t *testing.T) {
	resetIntents()
	t.Cleanup(resetIntents)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = NormalizeQuery(strings.Repeat("buy ", id%5) + "item")
				UpdateStopIntent([]string{"buy"})
				_ = NormalizeQuery("buy item")
			}
		}(i)
	}
	wg.Wait()
}
