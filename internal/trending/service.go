package trending

import (
	"sync"
)

type TrendingService struct {
    mu      sync.RWMutex
    buckets map[int64]map[string]int // ключ = unix timestamp минуты
    stopList map[string]struct{}
}

