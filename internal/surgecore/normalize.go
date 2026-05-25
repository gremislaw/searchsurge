package surgecore

import (
	"strings"
	"sync/atomic"
)

var defaultIntentWords = map[string]struct{}{
	"купить": {}, "заказать": {}, "дешевый": {}, "дешево": {},
	"обзор": {}, "найти": {}, "подобрать": {},
}

var intentWords atomic.Pointer[map[string]struct{}]

func init() {
	m := defaultIntentWords
	intentWords.Store(&m)
}

func NormalizeQuery(q string) string {
	q = strings.TrimSpace(strings.ToLower(q))
	if q == "" {
		return ""
	}

	stops := intentWords.Load()
	if stops == nil || !strings.Contains(q, " ") {
		return q
	}

	words := strings.Fields(q)
	if len(words) <= 1 {
		return q
	}

	var filtered []string
	for _, w := range words {
		if _, isIntent := (*stops)[w]; !isIntent {
			filtered = append(filtered, w)
		}
	}

	if len(filtered) == 0 {
		return q
	}
	return strings.Join(filtered, " ")
}

func UpdateStopIntent(words []string) {
	m := make(map[string]struct{}, len(words))
	for _, w := range words {
		m[strings.TrimSpace(strings.ToLower(w))] = struct{}{}
	}
	intentWords.Store(&m)
}
