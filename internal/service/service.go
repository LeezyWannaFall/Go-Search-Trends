package service

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/LeezyWannaFall/Go-Search-Trends/internal/model"
)

type TrendingService struct {
	mu       sync.RWMutex
	buckets  map[int64]map[string]int
	stopList map[string]struct{}
    lastCleanup int64
}

func New() *TrendingService {
	return &TrendingService{
		buckets:  make(map[int64]map[string]int),
		stopList: make(map[string]struct{}),
	}
}

func (s *TrendingService) Add(ctx context.Context, event model.SearchEvent) {
    s.mu.RLock()
    if _, ok := s.stopList[event.Query]; ok {
        s.mu.RUnlock()
        return
    }
    s.mu.RUnlock()

    currMin := event.Timestamp.Unix() / 60

    s.mu.Lock()
    if _, ok := s.buckets[currMin]; !ok {
        s.buckets[currMin] = make(map[string]int)
    }
    s.buckets[currMin][event.Query]++
    s.mu.Unlock()

    s.cleanupOldBuckets()
}

func (s *TrendingService) GetTop(ctx context.Context, n int) []model.TopEntry {
	if n <= 0 {
        return nil
    }

    var top []model.TopEntry

    tNow := time.Now()
    border := (tNow.Unix() / 60) - 5
    counts := make(map[string]int)

    s.mu.RLock()
    for key, bucket := range s.buckets {
        if key < border {
            continue
        }

        for query, cnt := range bucket {
            counts[query] += cnt
        }
    }
    s.mu.RUnlock()

    for key, val := range counts {
        top = append(top, model.TopEntry{Query: key, Count: val})
    }

    sort.Slice(top, func(i, j int) bool {
        if top[i].Count == top[j].Count {
            return top[i].Query < top[j].Query
        }
        return top[i].Count > top[j].Count
    })

    if len(top) > n {
        top = top[:n]
    }

    return top
}