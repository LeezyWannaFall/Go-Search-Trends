package service

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/LeezyWannaFall/Go-Search-Trends/internal/model"
)

type bucket map[string]map[string]struct{}

type TrendingService struct {
	mu       sync.RWMutex
	buckets  map[int64]bucket
	stopList map[string]struct{}
    lastCleanup int64
}

func New() *TrendingService {
	return &TrendingService{
		buckets:  make(map[int64]bucket),
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
        s.buckets[currMin] = make(bucket)
    }

    if _, ok := s.buckets[currMin][event.Query]; !ok {
        s.buckets[currMin][event.Query] = make(map[string]struct{})
    }

    s.buckets[currMin][event.Query][event.UserID] = struct{}{}
    s.mu.Unlock()

    s.cleanupOldBuckets()
}

func (s *TrendingService) GetTop(ctx context.Context, n int) []model.TopEntry {
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
            counts[query] += len(cnt)
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

func (s *TrendingService) AddWord(ctx context.Context, word string) {
    s.mu.Lock()
    defer s.mu.Unlock()

    s.stopList[word] = struct{}{}
}

func (s *TrendingService) DeleteWord(ctx context.Context, word string) {
    s.mu.Lock()
    defer s.mu.Unlock()

    delete(s.stopList, word)
}

func (s *TrendingService) GetBlackList(ctx context.Context) []string {
    s.mu.RLock()
    defer s.mu.RUnlock()

	blacklist := make([]string, 0, len(s.stopList))
    for word := range s.stopList {
        blacklist = append(blacklist, word)
    }

    return blacklist
}